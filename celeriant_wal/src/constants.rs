/// Per-batch event-type bloom size: one 32-byte split-block (see `celeriant_wal::sbbf`).
pub const BLOOM_BYTES: usize = 32;
#[cfg(not(feature = "small-metablock"))]
pub const FIXED_BLOCK_SIZE_BYTES: usize = 1024;
#[cfg(feature = "small-metablock")]
pub const FIXED_BLOCK_SIZE_BYTES: usize = 512;

pub const HEADER_BLOCK_SIZE_BYTES: usize = 512 * 1024;

#[cfg(not(feature = "small-metablock"))]
pub const MINIBATCH_SIZE_BYTES: usize = 718;
#[cfg(feature = "small-metablock")]
pub const MINIBATCH_SIZE_BYTES: usize = 206;
pub const WIRE_VERSION_WAL_METABLOCK: u32 = 1;
pub const WIRE_VERSION_WAL_DATABLOCK: u32 = 1;
pub const WIRE_VERSION_WAL_SHARD_LOG_HEADER: u32 = 1;
pub const WIRE_VERSION_S3_FALLBACK_BATCH: u32 = 1;
pub const WIRE_VERSION_SEGMENT_SUMMARY_BLOCK: u32 = 1;
pub const WIRE_SIZE_ENUM_DISCRIMINANT: usize = 4;
pub const FIRST_AGGREGATE_VERSION: u64 = 1;
/// Per-segment aggregate-key bloom size (256KB split-block; see `celeriant_wal::sbbf`).
/// ~10.5 bits/key at the 200k design capacity <1% false-positive rate.
pub const AGGREGATE_BLOOM_BYTES: usize = 256 * 1024;
pub type EntryHashBytes = [u8; 32];
pub const GENESIS_HASH: EntryHashBytes = [0u8; 32];
pub const STRUCT_TO_MEMORY_REAL_SIZE: usize = 3;

/// Minimum write alignment for DMA writes. Ensures writes cover full 4096-byte physical
/// sectors, avoiding read-modify-write penalties on NVMe/SSD devices that report 512-byte
/// logical sectors but use 4096-byte physical sectors.
pub const MIN_WRITE_ALIGNMENT: u64 = 4096;

/// Round `pos` up to the next multiple of `alignment`. Alignment must be a power of two.
pub const fn align_up(pos: u64, alignment: u64) -> u64 {
    (pos + alignment - 1) & !(alignment - 1)
}

/// Round `pos` down to the previous multiple of `alignment`. Alignment must be a power of two.
pub const fn align_down(pos: u64, alignment: u64) -> u64 {
    pos & !(alignment - 1)
}

/// Padding bytes needed to round `content_size` up to the next `MIN_WRITE_ALIGNMENT` boundary.
pub const fn write_padding(content_size: u64) -> u64 {
    align_up(content_size, MIN_WRITE_ALIGNMENT) - content_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_up_already_aligned() {
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(8192, 4096), 8192);
        assert_eq!(align_up(0, 4096), 0);
    }

    #[test]
    fn align_up_rounds_up() {
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(1024, 4096), 4096);
        assert_eq!(align_up(2048, 4096), 4096);
        assert_eq!(align_up(3072, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn align_down_already_aligned() {
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(8192, 4096), 8192);
        assert_eq!(align_down(0, 4096), 0);
    }

    #[test]
    fn align_down_rounds_down() {
        assert_eq!(align_down(1, 4096), 0);
        assert_eq!(align_down(4095, 4096), 0);
        assert_eq!(align_down(4097, 4096), 4096);
        assert_eq!(align_down(8191, 4096), 4096);
    }

    #[test]
    fn write_padding_metablock_counts() {
        assert_eq!(write_padding(1 * FIXED_BLOCK_SIZE_BYTES as u64), 3072);
        assert_eq!(write_padding(2 * FIXED_BLOCK_SIZE_BYTES as u64), 2048);
        assert_eq!(write_padding(3 * FIXED_BLOCK_SIZE_BYTES as u64), 1024);
        assert_eq!(write_padding(4 * FIXED_BLOCK_SIZE_BYTES as u64), 0);
        assert_eq!(write_padding(5 * FIXED_BLOCK_SIZE_BYTES as u64), 3072);
    }

    #[test]
    fn write_padding_zero() {
        assert_eq!(write_padding(0), 0);
    }

    #[test]
    fn write_padding_max_is_3072() {
        for count in 1..=100u64 {
            let padding = write_padding(count * FIXED_BLOCK_SIZE_BYTES as u64);
            assert!(padding <= 3072, "count={count}, padding={padding}");
            assert_eq!(padding % 1024, 0, "padding must be a multiple of 1024");
        }
    }

    #[test]
    fn rotation_guard_prevents_datablock_encroachment() {
        let metablocks_pos = 524288u64;
        let datablocks_pos = metablocks_pos + 8192;

        for metablock_count in 1..=4u64 {
            let content_metablocks = metablock_count * FIXED_BLOCK_SIZE_BYTES as u64;
            let padding = write_padding(content_metablocks);
            let padded_write_end = metablocks_pos + content_metablocks + padding;

            assert!(
                padded_write_end <= datablocks_pos,
                "count={metablock_count}: padded write at {padded_write_end} would overwrite datablocks at {datablocks_pos}"
            );
        }
    }

    #[test]
    fn rotation_guard_triggers_when_space_insufficient() {
        let available_space = 5000u64;

        // 4 metablocks: 4096 + 500 + 0 = 4596 < 5000 → fits
        let content = 4 * FIXED_BLOCK_SIZE_BYTES as u64;
        assert!(available_space >= content + 500 + write_padding(content));

        // 1 metablock: 1024 + 500 + 3072 = 4596 → also fits
        let content = 1 * FIXED_BLOCK_SIZE_BYTES as u64;
        assert!(available_space >= content + 500 + write_padding(content));

        // 3 metablocks + 2500 byte datablock: 3072 + 2500 + 1024 = 6596 > 5000 → rotate
        let content = 3 * FIXED_BLOCK_SIZE_BYTES as u64;
        assert!(available_space < content + 2500 + write_padding(content));
    }

    #[test]
    fn header_block_size_is_4096_aligned() {
        assert_eq!(HEADER_BLOCK_SIZE_BYTES % 4096, 0);
    }

    #[test]
    fn initial_positions_are_4096_aligned() {
        assert_eq!((HEADER_BLOCK_SIZE_BYTES as u64) % MIN_WRITE_ALIGNMENT, 0);
        let file_len = 1024 * 1024 * 1024u64;
        let datablocks_start = file_len - HEADER_BLOCK_SIZE_BYTES as u64;
        assert_eq!(datablocks_start % MIN_WRITE_ALIGNMENT, 0);
    }

    #[test]
    fn align_round_trip_at_boundaries() {
        for multiple in 0..=20u64 {
            let val = multiple * 4096;
            assert_eq!(align_up(val, 4096), val);
            assert_eq!(align_down(val, 4096), val);
        }
    }

    #[test]
    fn align_up_minus_align_down_bounded() {
        for pos in 0..=8200u64 {
            let up = align_up(pos, 4096);
            let down = align_down(pos, 4096);
            assert!(up - down <= 4096);
            assert!(up >= pos);
            assert!(down <= pos);
        }
    }
}