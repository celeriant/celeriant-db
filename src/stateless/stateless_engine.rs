use std::sync::OnceLock;

pub struct StatelessEngine {
    io_uring_queue_depth: u32,
    default_buffer_size: usize,
    io_uring_available: IoUringStatus,
}

static IO_URING_AVAILABLE: OnceLock<IoUringStatus> = OnceLock::new();

impl StatelessEngine {
    pub fn new() -> Self {
        Self {
            io_uring_queue_depth: 32,
            default_buffer_size: 8192,
            io_uring_available: Self::detect_io_uring_support(),
        }
    }
    
    fn detect_io_uring_support() -> IoUringStatus {
        *IO_URING_AVAILABLE.get_or_init(|| {
            #[cfg(target_os = "linux")]
            {
                // Try to create a minimal io_uring instance
                match io_uring::IoUring::new(1) {
                    Ok(_) => {
                        IoUringStatus::Available
                    }
                    Err(e) => {
                        IoUringStatus::Unavailable
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                IoUringStatus::UnsupportedPlatform
            }
        })
    }
    
    pub fn is_io_uring_available(&self) -> bool {
        self.io_uring_available == IoUringStatus::Available
    }
}


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IoUringStatus {
    Available,
    Unavailable,
    UnsupportedPlatform,
}