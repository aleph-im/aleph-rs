//! Default constants mirroring `aleph/toolkit/constants.py`.

/// 100 MiB — default upload limit for authenticated users.
pub const DEFAULT_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// 25 MiB — default upload limit for anonymous users.
pub const DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE: u64 = 25 * 1024 * 1024;
