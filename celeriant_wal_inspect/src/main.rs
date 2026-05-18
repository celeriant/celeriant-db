use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::ExitCode;

use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::metablocks::metablock::Metablock;
use celeriant_wal::metablocks::metablock_kind::MetablockKind;
use celeriant_wal::shard_log_header::ShardLogHeader;
use celeriant_wire::disk::versioned_block::{
    deserialise_metablock, deserialise_shard_log_header,
};

fn print_usage() {
    eprintln!(
        "celeriant-wal-inspect <log_file> <command>

Commands:
  header                       Print front + rear ShardLogHeader fields
  wal <wal_seq>              Find metablock with the given wal_seq
  range <start> <end>          Print all metablocks in [start..=end]
  bounds                       Print first and last metablock wal_seq in the file
  client <org_id> <agg_type_id> <agg_id> <client_id>
                               List every EventBatchMetadata batch matching
                               (org, agg_type, agg, client_id). IDs accept
                               decimal numbers OR 32-char hex strings
                               (matches the aggregate_key_str format printed
                               by the chaos audit report). Output one line per
                               batch: aggregate_version + min/max client_seq.

Examples:
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal header
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal wal 133939
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal range 133935 133942
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal \\
      client 00000000-0000-0000-0000-000000000001 \\
             00000000-0000-0000-0000-000000000001 \\
             00000000-0000-0000-0000-00000000003c 61"
    );
}

/// Accepts a u128 expressed either as a decimal integer or as a
/// 32-hex-character string (the audit report's `aggregate_key_str` form,
/// optionally with hyphens like a UUID). Returns the parsed value.
fn parse_u128_flexible(s: &str) -> Option<u128> {
    if let Ok(v) = s.parse::<u128>() {
        return Some(v);
    }
    let stripped: String = s.chars().filter(|c| *c != '-').collect();
    if stripped.len() == 32 {
        u128::from_str_radix(&stripped, 16).ok()
    } else {
        None
    }
}

fn hex32(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

fn read_header(file: &mut File, position: u64) -> Option<ShardLogHeader> {
    let mut buf = vec![0u8; HEADER_BLOCK_SIZE_BYTES];
    file.seek(SeekFrom::Start(position)).ok()?;
    file.read_exact(&mut buf).ok()?;
    deserialise_shard_log_header(&buf).ok()
}

fn print_header_block(label: &str, hdr: &ShardLogHeader) {
    println!("{label}:");
    println!("  metablocks_position           = {}", hdr.metablocks_position);
    println!("  datablocks_position           = {}", hdr.datablocks_position);
    println!("  wal_seq                     = {}", hdr.wal_seq);
    println!("  tip_hash                      = {}", hex32(&hdr.tip_hash));
    println!(
        "  last_received_repl_wal_seq  = {}",
        hdr.last_received_replication_wal_seq
    );
    let n_meta = (hdr
        .metablocks_position
        .saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64))
        / FIXED_BLOCK_SIZE_BYTES as u64;
    println!("  metablock_count               = {}", n_meta);
}

fn print_metablock(mb: &Metablock, file_offset: u64) {
    println!(
        "wal_seq = {} | lease = {} | offset = {} | server_ts = {} | node = {:032x}",
        mb.wal_seq, mb.lease_epoch, file_offset, mb.server_timestamp, mb.node_id
    );
    println!("  previous_tip_hash             = {}", hex32(&mb.previous_tip_hash));
    println!("  uncompressed_size             = {}", mb.uncompressed_size);
    println!("  compressed_size               = {}", mb.compressed_size);
    println!("  datablock_position            = {}", mb.datablock_position);
    println!("  kind                          = {:?}", mb.wal_metablock_type);
}

fn open_file(path: &PathBuf) -> std::io::Result<(File, u64)> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    Ok((file, len))
}

/// Iterate metablocks in [HEADER_BLOCK_SIZE_BYTES..metablocks_end] and pass each
/// (wal_seq, file_offset, deserialised_metablock) to the visitor. The visitor returns
/// `true` to keep going, `false` to stop.
fn for_each_metablock<F: FnMut(&Metablock, u64) -> bool>(
    file: &mut File,
    metablocks_end: u64,
    mut visit: F,
) -> std::io::Result<()> {
    let start = HEADER_BLOCK_SIZE_BYTES as u64;
    file.seek(SeekFrom::Start(start))?;
    let mut buf = [0u8; FIXED_BLOCK_SIZE_BYTES];
    let mut offset = start;
    while offset + FIXED_BLOCK_SIZE_BYTES as u64 <= metablocks_end {
        file.read_exact(&mut buf)?;
        if let Ok(mb) = deserialise_metablock(&buf) {
            if !visit(&mb, offset) {
                return Ok(());
            }
        }
        offset += FIXED_BLOCK_SIZE_BYTES as u64;
    }
    Ok(())
}

fn cmd_header(file: &mut File, file_len: u64) -> std::io::Result<()> {
    let front = read_header(file, 0);
    let rear_pos = file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64);
    let rear = read_header(file, rear_pos);
    println!("file_len = {}", file_len);
    println!();
    match front {
        Some(h) => print_header_block("front_header", &h),
        None => println!("front_header: <corrupt or missing>"),
    }
    println!();
    match rear {
        Some(h) => print_header_block("rear_header", &h),
        None => println!("rear_header: <corrupt or missing>"),
    }
    Ok(())
}

fn cmd_wal(file: &mut File, file_len: u64, target: u64) -> std::io::Result<()> {
    let hdr = read_header(file, 0)
        .or_else(|| read_header(file, file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64)));
    let metablocks_end = match hdr {
        Some(h) => h.metablocks_position,
        None => {
            eprintln!("ERROR: both headers corrupt; cannot determine metablock region");
            return Ok(());
        }
    };

    let mut found = false;
    for_each_metablock(file, metablocks_end, |mb, off| {
        if mb.wal_seq == target {
            print_metablock(mb, off);
            found = true;
            return false;
        }
        // metablocks are wal-ordered, stop scanning once past target
        if mb.wal_seq > target {
            return false;
        }
        true
    })?;
    if !found {
        println!("wal_seq {} not found in this file", target);
    }
    Ok(())
}

fn cmd_range(file: &mut File, file_len: u64, start: u64, end: u64) -> std::io::Result<()> {
    let hdr = read_header(file, 0)
        .or_else(|| read_header(file, file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64)));
    let metablocks_end = match hdr {
        Some(h) => h.metablocks_position,
        None => {
            eprintln!("ERROR: both headers corrupt; cannot determine metablock region");
            return Ok(());
        }
    };

    let mut printed = 0u64;
    for_each_metablock(file, metablocks_end, |mb, off| {
        if mb.wal_seq < start {
            return true;
        }
        if mb.wal_seq > end {
            return false;
        }
        print_metablock(mb, off);
        println!();
        printed += 1;
        true
    })?;
    println!("printed {} metablocks in [{}..={}]", printed, start, end);
    Ok(())
}

fn cmd_client(
    file: &mut File,
    file_len: u64,
    agg_key: &AggregateKey,
    client_id: u128,
) -> std::io::Result<()> {
    let hdr = read_header(file, 0)
        .or_else(|| read_header(file, file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64)));
    let (metablocks_end, read_metablocks_pos) = match hdr {
        Some(h) => (h.metablocks_position, h.metablocks_position),
        None => {
            eprintln!("ERROR: both headers corrupt; cannot determine metablock region");
            return Ok(());
        }
    };

    // Scan once for write-cursor-bounded records (everything up to file length).
    // Then mark which ones are within the read cursor.
    let mut batches: Vec<(u64, u64, u64, u64, u64, bool)> = Vec::new();
    // tuple: (wal_seq, aggregate_version, min_client_seq, max_client_seq, file_offset, within_read_cursor)
    let mut soft_deletes_seen = 0usize;
    let mut soft_trims_seen = 0usize;

    for_each_metablock(file, file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64), |mb, off| {
        // For empty/zeroed metablock slots, wal_seq is 0 and there's no point continuing.
        if mb.wal_seq == 0 && off > HEADER_BLOCK_SIZE_BYTES as u64 {
            return false;
        }
        match &mb.wal_metablock_type {
            MetablockKind::EventBatchMetadata(eb) => {
                if eb.aggregate_key == *agg_key && eb.client_id == client_id {
                    let within = off + FIXED_BLOCK_SIZE_BYTES as u64 <= read_metablocks_pos;
                    batches.push((
                        mb.wal_seq,
                        eb.aggregate_version,
                        eb.min_client_seq,
                        eb.max_client_seq,
                        off,
                        within,
                    ));
                }
            }
            MetablockKind::SoftDelete(sd) => {
                if sd.aggregate_key == *agg_key {
                    soft_deletes_seen += 1;
                    let within = off + FIXED_BLOCK_SIZE_BYTES as u64 <= read_metablocks_pos;
                    println!(
                        "[SoftDelete] wal_seq={} agg_version={} event_seq={} client_id={:032x} allow_recreate={} within_read={}",
                        mb.wal_seq, sd.aggregate_version, sd.event_seq, sd.client_id, sd.allow_recreate, within
                    );
                }
            }
            MetablockKind::SoftTrim(st) => {
                if st.aggregate_key == *agg_key {
                    soft_trims_seen += 1;
                    let within = off + FIXED_BLOCK_SIZE_BYTES as u64 <= read_metablocks_pos;
                    println!(
                        "[SoftTrim] wal_seq={} agg_version={} event_seq={} keep_from={} within_read={}",
                        mb.wal_seq, st.aggregate_version, st.event_seq, st.keep_from_aggregate_version, within
                    );
                }
            }
            MetablockKind::SchemaRegistration(_) => {}
        }
        true
    })?;

    println!("file_len           = {}", file_len);
    println!("metablocks_end (write) = {}", metablocks_end);
    println!("metablocks_end (read)  = {}", read_metablocks_pos);
    println!("aggregate_key      = org={} type={} id={}",
        agg_key.org_id, agg_key.aggregate_type_id, agg_key.aggregate_id);
    println!("client_id          = {}", client_id);
    println!("matched batches    = {}", batches.len());
    println!("matched soft deletes = {}", soft_deletes_seen);
    println!("matched soft trims = {}", soft_trims_seen);

    let mut min_seq: Option<u64> = None;
    let mut max_seq: Option<u64> = None;
    let mut max_version: Option<u64> = None;
    let mut count_within_read: u64 = 0;
    let mut all_client_seqs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for (wal_seq, agg_version, min_cs, max_cs, off, within) in &batches {
        if *within {
            count_within_read += 1;
        }
        for cs in *min_cs..=*max_cs {
            all_client_seqs.insert(cs);
        }
        match min_seq {
            None => min_seq = Some(*min_cs),
            Some(m) => min_seq = Some(m.min(*min_cs)),
        }
        match max_seq {
            None => max_seq = Some(*max_cs),
            Some(m) => max_seq = Some(m.max(*max_cs)),
        }
        match max_version {
            None => max_version = Some(*agg_version),
            Some(m) => max_version = Some(m.max(*agg_version)),
        }
        println!(
            "wal_seq={} agg_version={} client_seq=[{}..{}] offset={} within_read={}",
            wal_seq, agg_version, min_cs, max_cs, off, within
        );
    }
    println!();
    println!("summary:");
    println!("  total batches        = {}", batches.len());
    println!("  batches within read  = {}", count_within_read);
    println!("  batches past read    = {}", batches.len() as u64 - count_within_read);
    println!(
        "  min client_seq       = {}",
        min_seq.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
    );
    println!(
        "  max client_seq       = {}",
        max_seq.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
    );
    println!(
        "  max aggregate_version= {}",
        max_version.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
    );
    println!("  distinct client_seqs = {}", all_client_seqs.len());
    if let Some(max) = max_seq {
        let expected: std::collections::BTreeSet<u64> = (1..=max).collect();
        let missing: Vec<u64> = expected.difference(&all_client_seqs).copied().take(64).collect();
        if missing.is_empty() {
            println!("  missing in 1..={}    = none", max);
        } else {
            println!("  missing in 1..={}    = {:?}{}", max, missing,
                if missing.len() >= 64 { " (truncated)" } else { "" });
        }
    }
    Ok(())
}

fn cmd_bounds(file: &mut File, file_len: u64) -> std::io::Result<()> {
    let hdr = read_header(file, 0)
        .or_else(|| read_header(file, file_len.saturating_sub(HEADER_BLOCK_SIZE_BYTES as u64)));
    let metablocks_end = match hdr {
        Some(h) => h.metablocks_position,
        None => {
            eprintln!("ERROR: both headers corrupt");
            return Ok(());
        }
    };

    let mut first: Option<(u64, u64)> = None;
    let mut last: Option<(u64, u64)> = None;
    for_each_metablock(file, metablocks_end, |mb, off| {
        if first.is_none() {
            first = Some((mb.wal_seq, off));
        }
        last = Some((mb.wal_seq, off));
        true
    })?;
    match (first, last) {
        (Some((fw, fo)), Some((lw, lo))) => {
            println!("first metablock: wal_seq = {}, offset = {}", fw, fo);
            println!("last  metablock: wal_seq = {}, offset = {}", lw, lo);
            println!("count          = {}", lw.saturating_sub(fw) + 1);
        }
        _ => println!("no metablocks found"),
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        print_usage();
        return ExitCode::from(2);
    }

    let path = PathBuf::from(&args[1]);
    let cmd = args[2].as_str();

    let (mut file, file_len) = match open_file(&path) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("ERROR: cannot open {}: {}", path.display(), e);
            return ExitCode::from(1);
        }
    };

    let result = match cmd {
        "header" => cmd_header(&mut file, file_len),
        "bounds" => cmd_bounds(&mut file, file_len),
        "wal" => {
            let Some(arg) = args.get(3) else {
                print_usage();
                return ExitCode::from(2);
            };
            let Ok(wal) = arg.parse::<u64>() else {
                eprintln!("ERROR: wal_seq must be a u64");
                return ExitCode::from(2);
            };
            cmd_wal(&mut file, file_len, wal)
        }
        "range" => {
            let (Some(s), Some(e)) = (args.get(3), args.get(4)) else {
                print_usage();
                return ExitCode::from(2);
            };
            let (Ok(s), Ok(e)) = (s.parse::<u64>(), e.parse::<u64>()) else {
                eprintln!("ERROR: range start and end must be u64");
                return ExitCode::from(2);
            };
            cmd_range(&mut file, file_len, s, e)
        }
        "client" => {
            let (Some(org), Some(atype), Some(agg), Some(client)) =
                (args.get(3), args.get(4), args.get(5), args.get(6))
            else {
                print_usage();
                return ExitCode::from(2);
            };
            let Some(org_id) = parse_u128_flexible(org) else {
                eprintln!("ERROR: org_id must be a u128 (decimal) or 32-hex-char UUID-form");
                return ExitCode::from(2);
            };
            let Some(atype_id) = parse_u128_flexible(atype) else {
                eprintln!("ERROR: aggregate_type_id must be u128 or 32-hex-char");
                return ExitCode::from(2);
            };
            let Some(agg_id) = parse_u128_flexible(agg) else {
                eprintln!("ERROR: aggregate_id must be u128 or 32-hex-char");
                return ExitCode::from(2);
            };
            let Some(client_id) = parse_u128_flexible(client) else {
                eprintln!("ERROR: client_id must be u128 or 32-hex-char");
                return ExitCode::from(2);
            };
            let agg_key = AggregateKey::new(org_id, atype_id, agg_id);
            cmd_client(&mut file, file_len, &agg_key, client_id)
        }
        other => {
            eprintln!("ERROR: unknown command '{}'", other);
            print_usage();
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ERROR: {}", e);
            ExitCode::from(1)
        }
    }
}
