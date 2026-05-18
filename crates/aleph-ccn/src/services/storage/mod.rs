//! Object/file storage backends. Mirrors `aleph/services/storage/`.

pub mod engine;
pub mod filesystem_engine;
pub mod garbage_collector;
pub mod in_memory;
