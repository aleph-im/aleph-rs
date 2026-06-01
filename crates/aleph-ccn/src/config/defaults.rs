//! Default constants mirroring `aleph/toolkit/constants.py`.

/// 100 MiB — default upload limit for authenticated users.
pub const DEFAULT_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// 1 GiB — default authenticated IPFS file upload limit (`/ipfs/add_file`).
pub const DEFAULT_MAX_UPLOAD_FILE_SIZE: u64 = 1024 * 1024 * 1024;

/// 25 MiB — default upload limit for anonymous users.
pub const DEFAULT_MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE: u64 = 25 * 1024 * 1024;

/// 4 GiB — default CAR upload limit for authenticated directory imports.
pub const DEFAULT_MAX_UPLOAD_CAR_SIZE: u64 = 4 * 1024 * 1024 * 1024;
