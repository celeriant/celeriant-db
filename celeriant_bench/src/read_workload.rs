//! Age-stratified read and reheat cost accounting.
//!
//! No read benchmark exists anywhere else in the repo, and a uniform sample over
//! millions of keys would measure only the cold path and hide every cache. The
//! whole question here is *cost as a function of dormancy age*, so every
//! measurement lands in an age bucket and stays there.
//!
//! **A flat curve means cardinality scales. A curve rising with age means it does
//! not.** That single comparison is what the scenario exists to produce, which is
//! why this module keeps bytes-read alongside latency: a rise in latency with a
//! flat byte count is a queueing artefact, while a rise in both is the reverse
//! scan genuinely walking further.

use std::collections::BTreeMap;

use celeriant_client_tokio::client_error::ClientError;

use crate::population::AgeBucket;

/// Why a read failed, as a closed set so the tally is a fixed-size array rather
/// than a map keyed on formatted error strings.
///
/// The split that matters is client-side censoring versus a real server answer.
/// `RequestTimeout` means the read blew the client's own deadline and its
/// latency was never recorded, so the ops that *did* land are the survivors of
/// a truncated distribution — a percentile over them is survivorship bias, not
/// a result. `Wire`/`Read`/`Connect` instead say the connection died. Without
/// this split an error count says only "it failed" and the run is unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReadErrorKind {
    Connect,
    ConnectTimeout,
    RequestTimeout,
    Wire,
    Read,
    Protocol,
    NotLeader,
    ServerBusy,
    Server,
    /// The client read a response bound to a different request. Unlike every
    /// other kind here this one cannot be caused by load, partition or restart —
    /// it is proof of stream desync, so chaos asserts it stays at zero.
    CorrelationMismatch,
    Other,
}

impl ReadErrorKind {
    pub const ALL: [ReadErrorKind; 11] = [
        ReadErrorKind::Connect,
        ReadErrorKind::ConnectTimeout,
        ReadErrorKind::RequestTimeout,
        ReadErrorKind::Wire,
        ReadErrorKind::Read,
        ReadErrorKind::Protocol,
        ReadErrorKind::NotLeader,
        ReadErrorKind::ServerBusy,
        ReadErrorKind::Server,
        ReadErrorKind::CorrelationMismatch,
        ReadErrorKind::Other,
    ];

    pub fn of(e: &ClientError) -> Self {
        match e {
            ClientError::ConnectionFailed(_) => ReadErrorKind::Connect,
            ClientError::ConnectionTimeout => ReadErrorKind::ConnectTimeout,
            ClientError::RequestTimeout => ReadErrorKind::RequestTimeout,
            ClientError::WireError(_) => ReadErrorKind::Wire,
            ClientError::ReadError(_) => ReadErrorKind::Read,
            ClientError::ProtocolError => ReadErrorKind::Protocol,
            ClientError::NotLeader { .. } => ReadErrorKind::NotLeader,
            ClientError::ServerBusy => ReadErrorKind::ServerBusy,
            ClientError::Server(_) => ReadErrorKind::Server,
            ClientError::CorrelationMismatch { .. } => ReadErrorKind::CorrelationMismatch,
            _ => ReadErrorKind::Other,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ReadErrorKind::Connect => "connect",
            ReadErrorKind::ConnectTimeout => "connect-timeout",
            ReadErrorKind::RequestTimeout => "request-timeout",
            ReadErrorKind::Wire => "wire",
            ReadErrorKind::Read => "read",
            ReadErrorKind::Protocol => "protocol",
            ReadErrorKind::NotLeader => "not-leader",
            ReadErrorKind::ServerBusy => "server-busy",
            ReadErrorKind::Server => "server",
            ReadErrorKind::CorrelationMismatch => "correlation-mismatch",
            ReadErrorKind::Other => "other",
        }
    }
}

/// Bounded reservoir of latency samples with quantile estimates.
///
/// A five-hour run probing continuously would accumulate millions of samples, so
/// the digest holds a uniform reservoir instead. Uniform matters: the estimate
/// must not drift toward whenever the sample happened to be taken, because the
/// population is deliberately non-stationary and a late-biased sample would
/// report the end of the run as if it were the whole of it.
#[derive(Debug, Clone)]
pub struct LatencyDigest {
    cap: usize,
    samples: Vec<u64>,
    count: u64,
    rng: u64,
}

impl LatencyDigest {
    pub fn new(cap: usize, seed: u64) -> Self {
        Self { cap: cap.max(1), samples: Vec::new(), count: 0, rng: seed ^ 0x9E37_79B9_7F4A_7C15 }
    }

    pub fn record(&mut self, micros: u64) {
        self.count += 1;
        if self.samples.len() < self.cap {
            self.samples.push(micros);
            return;
        }
        let j = self.next_rand() % self.count;
        if j < self.cap as u64 {
            self.samples[j as usize] = micros;
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn retained(&self) -> usize {
        self.samples.len()
    }

    /// Nearest-rank quantile over the retained sample. `None` when nothing was
    /// recorded — an empty bucket must read as "no data", never as zero latency.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut s = self.samples.clone();
        s.sort_unstable();
        let q = q.clamp(0.0, 1.0);
        let idx = ((s.len() as f64 - 1.0) * q).round() as usize;
        Some(s[idx])
    }

    pub fn p50(&self) -> Option<u64> {
        self.quantile(0.50)
    }
    pub fn p99(&self) -> Option<u64> {
        self.quantile(0.99)
    }
    pub fn max(&self) -> Option<u64> {
        self.samples.iter().copied().max()
    }

    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Debug, Clone)]
pub struct BucketStats {
    pub latency: LatencyDigest,
    pub bytes_read: u64,
    pub ops: u64,
    pub errors: u64,
    /// Successful reads that came back with zero bytes.
    ///
    /// Discriminates the two ways a suspiciously fast cold read can happen. If
    /// a sub-millisecond p50 is made of empty responses, the data was not
    /// visible and that is a correctness signal. If the responses carried
    /// bytes, a sub-millisecond round trip is below this cluster's floor and
    /// the measurement is wrong. Without this the two are indistinguishable
    /// and the cold/warm ratio cannot be read at all.
    pub empty_reads: u64,
    /// `errors` split by `ReadErrorKind`, indexed by `ReadErrorKind::ALL`.
    pub error_kinds: [u64; ReadErrorKind::ALL.len()],
}

impl BucketStats {
    fn new(seed: u64) -> Self {
        Self {
            latency: LatencyDigest::new(DIGEST_CAP, seed),
            bytes_read: 0,
            ops: 0,
            errors: 0,
            empty_reads: 0,
            error_kinds: [0; ReadErrorKind::ALL.len()],
        }
    }

    /// Mean bytes per successful op. `None` when no op succeeded, so a bucket
    /// that only ever errored cannot masquerade as a cheap read.
    pub fn bytes_per_read(&self) -> Option<u64> {
        (self.ops > 0).then(|| self.bytes_read / self.ops)
    }
}

const DIGEST_CAP: usize = 10_000;

/// One age bucket, as numbers rather than table cells.
///
/// Latencies stay in microseconds — the markdown formats to milliseconds, the
/// machine-readable form must not lose the resolution — and stay `Option`:
/// a bucket the run never reached reads as null, never as `0`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReheatBucketRow {
    pub bucket: AgeBucket,
    pub ops: u64,
    pub empty: u64,
    pub errors: u64,
    pub p50_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub max_us: Option<u64>,
    pub bytes_per_read: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ErrorKindCount {
    pub kind: ReadErrorKind,
    pub count: u64,
}

/// A whole reheat cost curve, machine-readable. The markdown is rendered from
/// this, so the report and the run JSON cannot drift apart.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReheatCurveJson {
    pub label: String,
    pub buckets: Vec<ReheatBucketRow>,
    /// Whole-curve errors split by kind, heaviest first. Empty when clean.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_kinds: Vec<ErrorKindCount>,
}

/// Cold versus warm for one bucket. Same keys, same concurrency, so `ratio` is
/// the price of an empty cache.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReheatDeltaRow {
    pub bucket: AgeBucket,
    pub p50_before_us: Option<u64>,
    pub p50_after_us: Option<u64>,
    /// `after / before`. `None` when either side is missing or the baseline
    /// rounded to zero microseconds — an infinity there reads as a
    /// catastrophic regression that never happened.
    pub ratio: Option<f64>,
    pub bytes_before: Option<u64>,
    pub bytes_after: Option<u64>,
}

fn ms(v: Option<u64>) -> String {
    v.map(|x| format!("{:.2}ms", x as f64 / 1000.0)).unwrap_or_else(|| "—".into())
}

fn num(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "—".into())
}

impl ReheatCurveJson {
    /// Markdown table. Empty buckets render `—`, never `0ms`: a missing row and
    /// a zero row mean very different things and the report must not blur them.
    pub fn to_markdown(&self) -> String {
        let mut md = format!("### {}\n\n", self.label);
        md.push_str("| Age bucket | ops | empty | errors | p50 | p99 | max | bytes/read |\n");
        md.push_str("|---|---|---|---|---|---|---|---|\n");
        for r in &self.buckets {
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
                r.bucket.name(),
                r.ops,
                r.empty,
                r.errors,
                ms(r.p50_us),
                ms(r.p99_us),
                ms(r.max_us),
                num(r.bytes_per_read),
            ));
        }
        let by_kind = self.error_summary();
        if !by_kind.is_empty() {
            md.push_str(&format!("\nErrors by kind: {by_kind}\n"));
        }
        let censored = self.request_timeouts();
        if censored > 0 {
            md.push_str(&format!(
                "\n**CENSORED — not a measurement.** {censored} read(s) hit the client request \
                 deadline and recorded no latency sample at all. Every percentile above is a \
                 lower bound over the reads that happened to finish in time.\n"
            ));
        }
        md
    }

    /// Reads that blew the client's own deadline, so contributed no latency
    /// sample. Non-zero means this curve is a censored distribution and its
    /// percentiles are lower bounds, not measurements.
    pub fn request_timeouts(&self) -> u64 {
        self.error_kinds
            .iter()
            .find(|e| e.kind == ReadErrorKind::RequestTimeout)
            .map_or(0, |e| e.count)
    }

    /// Reads that came back bound to a different request. Load, partitions and
    /// restarts cannot produce this; only a desynchronised stream can.
    pub fn correlation_mismatches(&self) -> u64 {
        self.error_kinds
            .iter()
            .find(|e| e.kind == ReadErrorKind::CorrelationMismatch)
            .map_or(0, |e| e.count)
    }

    pub fn error_summary(&self) -> String {
        self.error_kinds
            .iter()
            .map(|e| format!("{} {}", e.kind.name(), e.count))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The deliverable: per age bucket, what a first touch of a dormant aggregate cost.
#[derive(Debug, Clone)]
pub struct ReheatCostCurve {
    label: String,
    buckets: BTreeMap<&'static str, BucketStats>,
}

impl ReheatCostCurve {
    pub fn new(label: impl Into<String>, seed: u64) -> Self {
        let mut buckets = BTreeMap::new();
        for (i, b) in AgeBucket::ALL.iter().enumerate() {
            buckets.insert(b.name(), BucketStats::new(seed ^ (i as u64).wrapping_mul(0x9E37_79B9)));
        }
        Self { label: label.into(), buckets }
    }

    pub fn record(&mut self, bucket: AgeBucket, micros: u64, bytes: u64) {
        if let Some(s) = self.buckets.get_mut(bucket.name()) {
            s.latency.record(micros);
            s.bytes_read += bytes;
            s.ops += 1;
            if bytes == 0 {
                s.empty_reads += 1;
            }
        }
    }

    pub fn record_error(&mut self, bucket: AgeBucket, kind: ReadErrorKind) {
        if let Some(s) = self.buckets.get_mut(bucket.name()) {
            s.errors += 1;
            s.error_kinds[kind as usize] += 1;
        }
    }

    /// Error kinds across every bucket, heaviest first. Empty when nothing
    /// failed, so callers can print it unconditionally.
    pub fn error_summary(&self) -> String {
        self.to_json().error_summary()
    }

    /// The machine-readable form. Everything the report says about this curve
    /// is rendered from here, so the JSON and the markdown cannot disagree.
    pub fn to_json(&self) -> ReheatCurveJson {
        let mut tally = [0u64; ReadErrorKind::ALL.len()];
        for s in self.buckets.values() {
            for (slot, n) in tally.iter_mut().zip(s.error_kinds) {
                *slot += n;
            }
        }
        let mut error_kinds: Vec<ErrorKindCount> = ReadErrorKind::ALL
            .into_iter()
            .zip(tally)
            .filter(|(_, count)| *count > 0)
            .map(|(kind, count)| ErrorKindCount { kind, count })
            .collect();
        error_kinds.sort_unstable_by_key(|e| std::cmp::Reverse(e.count));

        ReheatCurveJson {
            label: self.label.clone(),
            buckets: AgeBucket::ALL
                .iter()
                .filter_map(|b| {
                    let s = self.buckets.get(b.name())?;
                    Some(ReheatBucketRow {
                        bucket: *b,
                        ops: s.ops,
                        empty: s.empty_reads,
                        errors: s.errors,
                        p50_us: s.latency.p50(),
                        p99_us: s.latency.p99(),
                        max_us: s.latency.max(),
                        bytes_per_read: s.bytes_per_read(),
                    })
                })
                .collect(),
            error_kinds,
        }
    }

    pub fn stats(&self, bucket: AgeBucket) -> Option<&BucketStats> {
        self.buckets.get(bucket.name())
    }

    /// Buckets holding at least `min_ops` successful samples. `AgeSpreadReached`
    /// asserts on this: without spread there is no curve, only a blended number.
    pub fn populated_buckets(&self, min_ops: u64) -> Vec<AgeBucket> {
        AgeBucket::ALL
            .iter()
            .copied()
            .filter(|b| self.buckets.get(b.name()).is_some_and(|s| s.ops >= min_ops))
            .collect()
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn to_markdown(&self) -> String {
        self.to_json().to_markdown()
    }

    /// Reads censored by the client deadline. See
    /// [`ReheatCurveJson::request_timeouts`].
    pub fn request_timeouts(&self) -> u64 {
        self.buckets
            .values()
            .map(|s| s.error_kinds[ReadErrorKind::RequestTimeout as usize])
            .sum()
    }
}

/// Compare the same curve before and after a restart. This is
/// `ColdRestartReheatDelta`: identical buckets, identical concurrency, so the
/// only thing that changed is that every cache is empty.
pub fn reheat_delta_rows(before: &ReheatCostCurve, after: &ReheatCostCurve) -> Vec<ReheatDeltaRow> {
    AgeBucket::ALL
        .into_iter()
        .filter_map(|bucket| {
            let (x, y) = (before.stats(bucket)?, after.stats(bucket)?);
            let (p50_before_us, p50_after_us) = (x.latency.p50(), y.latency.p50());
            Some(ReheatDeltaRow {
                bucket,
                p50_before_us,
                p50_after_us,
                // Guard the denominator: a sub-millisecond warm p50 can round
                // to zero microseconds on a fast path.
                ratio: match (p50_before_us, p50_after_us) {
                    (Some(a), Some(c)) if a > 0 => Some(c as f64 / a as f64),
                    _ => None,
                },
                bytes_before: x.bytes_per_read(),
                bytes_after: y.bytes_per_read(),
            })
        })
        .collect()
}

pub fn reheat_delta_markdown(before: &ReheatCostCurve, after: &ReheatCostCurve) -> String {
    delta_markdown(&reheat_delta_rows(before, after), before.request_timeouts(), after.request_timeouts())
}

/// The delta table, with every ratio marked when either side's distribution was
/// censored by the client deadline.
///
/// A ratio of two percentiles is only a price if both percentiles are
/// measurements. When reads timed out they contributed no sample, so the side
/// that censored reports a lower bound and the ratio is arithmetic over an
/// unknown — which is far more dangerous than a blank cell, because it renders
/// as a finding. The `†` and its footnote exist so nobody can quote the number
/// without also quoting what is wrong with it.
pub fn delta_markdown(rows: &[ReheatDeltaRow], before_timeouts: u64, after_timeouts: u64) -> String {
    let censored = before_timeouts > 0 || after_timeouts > 0;
    let mut md = String::from("### Cold-restart reheat delta (after / before)\n\n");
    md.push_str("| Age bucket | p50 before | p50 after | ratio | bytes before | bytes after |\n");
    md.push_str("|---|---|---|---|---|---|\n");
    for r in rows {
        let ratio = match r.ratio {
            Some(v) if censored => format!("{v:.2}x †"),
            Some(v) => format!("{v:.2}x"),
            None => "—".into(),
        };
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            r.bucket.name(),
            ms(r.p50_before_us),
            ms(r.p50_after_us),
            ratio,
            num(r.bytes_before),
            num(r.bytes_after),
        ));
    }
    if censored {
        md.push_str(&format!(
            "\n† **Not a measurement.** The latency distribution behind these ratios is censored \
             at the client request deadline: {before_timeouts} request timeout(s) before the \
             restart, {after_timeouts} after. A timed-out read records no latency, so each p50 \
             above is a lower bound over the survivors and the ratio is arithmetic over an \
             unknown. Raise the read deadline and re-run before quoting any of it.\n"
        ));
    }
    md
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_exact_below_the_reservoir_cap() {
        let mut d = LatencyDigest::new(10_000, 7);
        for v in 1..=100u64 {
            d.record(v);
        }
        assert_eq!(d.count(), 100);
        assert_eq!(d.retained(), 100);
        // Nearest-rank on `(n-1)*q`: for 1..=100 the median index is 50, i.e. 51.
        // Pinned to the convention rather than to an intuition about "the middle",
        // so a later change of definition shows up here instead of quietly moving
        // every published percentile.
        assert_eq!(d.p50(), Some(51));
        assert_eq!(d.p99(), Some(99));
        assert_eq!(d.max(), Some(100));
    }

    #[test]
    fn an_empty_bucket_reads_as_no_data_not_as_zero_latency() {
        // A bucket the run never reached must not report 0ms — that would draw
        // a flat curve out of missing data, which is the exact false negative
        // this scenario is built to avoid.
        let d = LatencyDigest::new(16, 1);
        assert_eq!(d.p50(), None);
        assert_eq!(d.p99(), None);
        assert_eq!(d.max(), None);

        let c = ReheatCostCurve::new("x", 1);
        assert!(c.populated_buckets(1).is_empty());
        assert_eq!(c.stats(AgeBucket::FourHour).unwrap().bytes_per_read(), None);
    }

    #[test]
    fn the_digest_is_bounded_but_keeps_counting() {
        let mut d = LatencyDigest::new(100, 42);
        for v in 0..50_000u64 {
            d.record(v);
            assert!(d.retained() <= 100);
        }
        assert_eq!(d.count(), 50_000);
        assert_eq!(d.retained(), 100);
    }

    #[test]
    fn the_reservoir_spans_the_run_rather_than_one_end_of_it() {
        // A late- or early-biased sample would report one phase of a
        // deliberately non-stationary run as if it were the whole of it.
        let mut d = LatencyDigest::new(200, 99);
        for v in 0..20_000u64 {
            d.record(v);
        }
        let lo = d.samples.iter().filter(|&&v| v < 5_000).count();
        let hi = d.samples.iter().filter(|&&v| v >= 15_000).count();
        assert!(lo > 10, "no early samples retained: {lo}");
        assert!(hi > 10, "no late samples retained: {hi}");
    }

    #[test]
    fn buckets_stay_separate_and_track_bytes_independently() {
        let mut c = ReheatCostCurve::new("reheat", 5);
        c.record(AgeBucket::FiveMin, 1_000, 4_096);
        c.record(AgeBucket::FiveMin, 3_000, 8_192);
        c.record(AgeBucket::FourHour, 90_000, 1_048_576);
        c.record_error(AgeBucket::TwoHour, ReadErrorKind::RequestTimeout);

        let young = c.stats(AgeBucket::FiveMin).unwrap();
        assert_eq!(young.ops, 2);
        assert_eq!(young.bytes_per_read(), Some(6_144));
        let old = c.stats(AgeBucket::FourHour).unwrap();
        assert_eq!(old.bytes_per_read(), Some(1_048_576));
        assert_eq!(c.stats(AgeBucket::TwoHour).unwrap().errors, 1);
        // An errored-only bucket has no cost to report; it must not read cheap.
        assert_eq!(c.stats(AgeBucket::TwoHour).unwrap().bytes_per_read(), None);
        assert_eq!(c.populated_buckets(1), vec![AgeBucket::FiveMin, AgeBucket::FourHour]);
        assert_eq!(c.populated_buckets(2), vec![AgeBucket::FiveMin]);
    }

    #[test]
    fn the_error_tally_names_the_dominant_kind() {
        // The phase-5 failure this exists for: a 99% error rate that the old
        // bare count rendered as "11908 errors" and nothing else. The summary
        // has to lead with whichever kind actually dominated.
        let mut c = ReheatCostCurve::new("cold", 7);
        for _ in 0..10 {
            c.record_error(AgeBucket::FiveMin, ReadErrorKind::RequestTimeout);
        }
        c.record_error(AgeBucket::TwoHour, ReadErrorKind::RequestTimeout);
        c.record_error(AgeBucket::FiveMin, ReadErrorKind::Wire);

        assert_eq!(c.error_summary(), "request-timeout 11, wire 1");
        assert_eq!(c.stats(AgeBucket::FiveMin).unwrap().errors, 11);
        assert!(c.to_markdown().contains("Errors by kind: request-timeout 11, wire 1"));

        // A clean curve prints nothing rather than an empty tail.
        assert_eq!(ReheatCostCurve::new("warm", 7).error_summary(), "");
    }

    #[test]
    fn every_client_error_maps_to_a_distinct_named_kind() {
        // `record_error` indexes the tally by `kind as usize`, so a mismatch
        // between declaration order and `ALL` would silently mis-attribute.
        for (i, k) in ReadErrorKind::ALL.into_iter().enumerate() {
            assert_eq!(k as usize, i, "{} is out of order in ALL", k.name());
        }
        assert_eq!(ReadErrorKind::of(&ClientError::RequestTimeout), ReadErrorKind::RequestTimeout);
        assert_eq!(ReadErrorKind::of(&ClientError::ConnectionTimeout), ReadErrorKind::ConnectTimeout);
        assert_eq!(ReadErrorKind::of(&ClientError::ProtocolError), ReadErrorKind::Protocol);
        assert_eq!(ReadErrorKind::of(&ClientError::ServerBusy), ReadErrorKind::ServerBusy);
        assert_eq!(
            ReadErrorKind::of(&ClientError::ConnectionFailed(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset
            ))),
            ReadErrorKind::Connect
        );
    }

    #[test]
    fn the_serialised_name_is_the_printed_name() {
        // Two spellings of every kind — `name()` for the markdown, serde's
        // rename for the JSON. A drift would make the report and the run JSON
        // disagree about which failure dominated.
        for k in ReadErrorKind::ALL {
            assert_eq!(serde_json::to_value(k).unwrap(), serde_json::json!(k.name()));
        }
        for b in AgeBucket::ALL {
            assert_eq!(serde_json::to_value(b).unwrap(), serde_json::json!(b.name()));
        }
    }

    #[test]
    fn a_rising_curve_is_visible_in_the_delta_table() {
        // The headline read: cost rising with dormancy age after a cold restart.
        let mut before = ReheatCostCurve::new("warm", 1);
        let mut after = ReheatCostCurve::new("cold", 1);
        before.record(AgeBucket::FiveMin, 1_000, 1_024);
        after.record(AgeBucket::FiveMin, 4_000, 4_096);
        let md = reheat_delta_markdown(&before, &after);
        assert!(md.contains("4.00x"), "{md}");
        // Buckets present in neither run must not invent a ratio.
        assert!(md.contains("| 4h | — | — | — | — | — |"), "{md}");
    }

    #[test]
    fn a_censored_ratio_is_marked_and_an_uncensored_one_is_left_clean() {
        // A ratio of two percentiles is a price only when both are
        // measurements. One timed-out read makes it arithmetic over a lower
        // bound, and the table must not render it as a finding.
        let mut warm = ReheatCostCurve::new("warm", 1);
        let mut cold = ReheatCostCurve::new("cold", 1);
        warm.record(AgeBucket::FiveMin, 1_000, 1_024);
        cold.record(AgeBucket::FiveMin, 4_000, 4_096);

        let clean = reheat_delta_markdown(&warm, &cold);
        assert!(clean.contains("| 5m | 1.00ms | 4.00ms | 4.00x |"), "{clean}");
        assert!(!clean.contains('†'), "{clean}");
        assert!(!cold.to_markdown().contains("CENSORED"));

        cold.record_error(AgeBucket::FiveMin, ReadErrorKind::RequestTimeout);
        let marked = reheat_delta_markdown(&warm, &cold);
        assert!(marked.contains("| 5m | 1.00ms | 4.00ms | 4.00x † |"), "{marked}");
        assert!(marked.contains("† **Not a measurement.**"), "{marked}");
        assert!(marked.contains("0 request timeout(s) before the restart, 1 after"), "{marked}");
        // A bucket with no ratio stays blank rather than acquiring a mark.
        assert!(marked.contains("| 4h | — | — | — | — | — |"), "{marked}");
        // And the curve says so where its own percentiles are printed.
        assert!(cold.to_markdown().contains("**CENSORED — not a measurement.**"));
        assert_eq!(cold.request_timeouts(), 1);
        assert_eq!(cold.to_json().request_timeouts(), 1);
        assert_eq!(warm.request_timeouts(), 0);
    }

    #[test]
    fn a_zero_microsecond_baseline_does_not_print_an_infinite_ratio() {
        let mut before = ReheatCostCurve::new("warm", 1);
        let mut after = ReheatCostCurve::new("cold", 1);
        before.record(AgeBucket::FiveMin, 0, 1);
        after.record(AgeBucket::FiveMin, 5_000, 1);
        assert!(reheat_delta_markdown(&before, &after).contains("| 5m | 0.00ms | 5.00ms | — |"));
    }
}
