//! In-process caches. Mirrors `aleph/cache.py` (process-local) and
//! `aleph/services/cache/node_cache.py` (Redis-backed, ported separately).

pub mod local;
pub mod node_cache;
