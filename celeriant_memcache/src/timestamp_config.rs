use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampPrecision {
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

#[derive(Clone, Copy, Debug)]
pub struct TimestampConfig {
    pub precision: TimestampPrecision,
    /// Seconds offset from Unix epoch to custom epoch.
    /// Positive = custom epoch is after 1970, negative = before 1970.
    pub epoch_offset_secs: i64,
}

impl Default for TimestampConfig {
    fn default() -> Self {
        Self {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: 0,
        }
    }
}

impl TimestampConfig {
    /// Returns current time as u64 according to configured precision and epoch.
    /// 
    /// # Panics
    /// Panics if current time is before the configured custom epoch.
    pub fn now(&self) -> u64 {
        let mut since_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before Unix epoch");

        if self.epoch_offset_secs != 0 {
            since_unix = if self.epoch_offset_secs >= 0 {
                since_unix
                    .checked_sub(Duration::from_secs(self.epoch_offset_secs as u64))
                    .expect("Current time is before configured custom epoch")
            } else {
                since_unix + Duration::from_secs(self.epoch_offset_secs.unsigned_abs())
            };
        }

        match self.precision {
            TimestampPrecision::Milliseconds => since_unix.as_millis() as u64,
            TimestampPrecision::Microseconds => since_unix.as_micros() as u64,
            TimestampPrecision::Nanoseconds => since_unix.as_nanos() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TimestampConfig::default();
        assert_eq!(config.precision, TimestampPrecision::Milliseconds);
        assert_eq!(config.epoch_offset_secs, 0);
    }

    #[test]
    fn test_now_returns_reasonable_value() {
        let config = TimestampConfig::default();
        let now = config.now();
        
        // Should be after 2024-01-01 in milliseconds (1704067200000)
        assert!(now > 1_704_067_200_000);
    }

    #[test]
    fn test_now_precision_ordering() {
        let config_ms = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: 0,
        };
        let config_us = TimestampConfig {
            precision: TimestampPrecision::Microseconds,
            epoch_offset_secs: 0,
        };
        let config_ns = TimestampConfig {
            precision: TimestampPrecision::Nanoseconds,
            epoch_offset_secs: 0,
        };

        let ms = config_ms.now();
        let us = config_us.now();
        let ns = config_ns.now();

        // Higher precision should yield larger numbers
        assert!(us > ms);
        assert!(ns > us);
        
        // Verify approximate ratios (allowing for timing variance)
        assert!(us / ms >= 900 && us / ms <= 1100); // ~1000x
        assert!(ns / us >= 900 && ns / us <= 1100); // ~1000x
    }

    #[test]
    fn test_positive_epoch_offset() {
        let config_no_offset = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: 0,
        };
        let config_with_offset = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: 1_000_000, // ~11.5 days in the future from unix epoch
        };

        let now_no_offset = config_no_offset.now();
        let now_with_offset = config_with_offset.now();

        // With positive offset, timestamp should be smaller
        assert!(now_with_offset < now_no_offset);
        
        // Difference should be approximately 1_000_000 seconds in milliseconds
        let diff = now_no_offset - now_with_offset;
        assert!((diff as i64 - 1_000_000_000).abs() < 1000); // within 1 second tolerance
    }

    #[test]
    fn test_negative_epoch_offset() {
        let config_no_offset = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: 0,
        };
        let config_with_offset = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: -1_000_000, // before unix epoch
        };

        let now_no_offset = config_no_offset.now();
        let now_with_offset = config_with_offset.now();

        // With negative offset, timestamp should be larger
        assert!(now_with_offset > now_no_offset);
        
        // Difference should be approximately 1_000_000 seconds in milliseconds
        let diff = now_with_offset - now_no_offset;
        assert!((diff as i64 - 1_000_000_000).abs() < 1000);
    }

    #[test]
    #[should_panic(expected = "Current time is before configured custom epoch")]
    fn test_panics_when_time_before_custom_epoch() {
        let config = TimestampConfig {
            precision: TimestampPrecision::Milliseconds,
            epoch_offset_secs: i64::MAX, // far in the future
        };
        config.now();
    }
}