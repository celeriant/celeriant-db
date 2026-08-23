use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::config::ClusterConfig;
use crate::sample::{NodeSample, elapsed_ms, parse_metrics, scrape_interval};

#[derive(Clone, Default)]
pub struct SampleStore {
    inner: Arc<Mutex<Vec<NodeSample>>>,
}

impl SampleStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn push(&self, s: NodeSample) {
        self.inner.lock().await.push(s);
    }

    pub async fn snapshot(&self) -> Vec<NodeSample> {
        self.inner.lock().await.clone()
    }
}

pub struct Scraper {
    store: SampleStore,
    handle: JoinHandle<()>,
    stop: Arc<tokio::sync::Notify>,
    wall_start: SystemTime,
}

impl Scraper {
    pub fn start(cfg: &ClusterConfig) -> Self {
        let store = SampleStore::new();
        let stop = Arc::new(tokio::sync::Notify::new());
        let wall_start = SystemTime::now();
        let leader_url = cfg.metrics_url(&cfg.leader_host);
        let follower_url = cfg.metrics_url(&cfg.follower_host);
        let leader_host = cfg.leader_host.clone();
        let follower_host = cfg.follower_host.clone();

        let store_clone = store.clone();
        let stop_clone = stop.clone();
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(400))
                .build()
                .expect("reqwest client");
            let start = Instant::now();
            let interval = scrape_interval();
            // NeverAhead needs the current FOLLOWER sampled strictly before the
            // current leader within a tick: sampled follower-first, any state
            // change between the two scrapes only advances the leader's read
            // cursor, so cross-node skew can bias the comparison toward pass
            // but never fake a violation. Order follows the previous tick's
            // node_role and so swaps one tick late after a promotion — that
            // tick falls inside the invariant's stability guard.
            let mut config_leader_leads = true;
            // Interning table for `metric_keys_present`, one slot per host. The
            // set changes only when a counter registers for the first time, so
            // after the first few ticks every sample can share one allocation
            // instead of carrying its own 78-entry BTreeSet<String>. Without
            // this a 5-hour run retains ~74,000 owned copies, and `snapshot()`
            // clones the whole Vec with two or three clones live at once.
            let mut interned: std::collections::HashMap<String, std::sync::Arc<std::collections::BTreeSet<String>>> =
                std::collections::HashMap::new();
            loop {
                let t_ms = elapsed_ms(start, Instant::now());
                let (leader_sample, follower_sample) = if config_leader_leads {
                    let f = scrape_one(&client, &follower_host, &follower_url, t_ms).await;
                    let l = scrape_one(&client, &leader_host, &leader_url, t_ms).await;
                    (l, f)
                } else {
                    let l = scrape_one(&client, &leader_host, &leader_url, t_ms).await;
                    let f = scrape_one(&client, &follower_host, &follower_url, t_ms).await;
                    (l, f)
                };
                if leader_sample.ok && leader_sample.node_role >= 0.5 {
                    config_leader_leads = true;
                } else if follower_sample.ok && follower_sample.node_role >= 0.5 {
                    config_leader_leads = false;
                }
                let mut leader_sample = leader_sample;
                let mut follower_sample = follower_sample;
                for sample in [&mut leader_sample, &mut follower_sample] {
                    match interned.get(&sample.host) {
                        Some(prev) if **prev == *sample.metric_keys_present => {
                            sample.metric_keys_present = std::sync::Arc::clone(prev);
                        }
                        _ => {
                            interned.insert(sample.host.clone(), std::sync::Arc::clone(&sample.metric_keys_present));
                        }
                    }
                }
                store_clone.push(leader_sample).await;
                store_clone.push(follower_sample).await;

                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stop_clone.notified() => break,
                }
            }
        });

        Self { store, handle, stop, wall_start }
    }

    pub fn store(&self) -> SampleStore {
        self.store.clone()
    }

    /// Stops the scraper and returns the captured store along with the wall-clock
    /// timestamps that bracket the scrape window.
    pub async fn stop(self) -> ScraperOutcome {
        self.stop.notify_one();
        let _ = self.handle.await;
        ScraperOutcome { store: self.store, wall_start: self.wall_start, wall_end: SystemTime::now() }
    }
}

pub struct ScraperOutcome {
    pub store: SampleStore,
    pub wall_start: SystemTime,
    pub wall_end: SystemTime,
}

async fn scrape_one(client: &reqwest::Client, host: &str, url: &str, t_ms: u64) -> NodeSample {
    match client.get(url).send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                return NodeSample::unreachable(host.to_string(), t_ms, format!("HTTP {status}"));
            }
            match resp.text().await {
                Ok(body) => parse_metrics(host.to_string(), t_ms, &body),
                Err(e) => NodeSample::unreachable(host.to_string(), t_ms, format!("body: {e}")),
            }
        }
        Err(e) => NodeSample::unreachable(host.to_string(), t_ms, e.to_string()),
    }
}
