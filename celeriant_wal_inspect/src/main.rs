use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::ExitCode;

use celeriant_wal::constants::{FIXED_BLOCK_SIZE_BYTES, HEADER_BLOCK_SIZE_BYTES};
use celeriant_wal::metablocks::metablock::Metablock;
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

Examples:
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal header
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal wal 133939
  celeriant-wal-inspect /var/lib/celeriant/shard_003/log_1.wal range 133935 133942"
    );
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
