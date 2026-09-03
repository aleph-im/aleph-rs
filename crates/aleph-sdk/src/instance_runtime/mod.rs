//! Client-side model for the aleph-instance-runtime/1 manifest, the format
//! SEV-SNP confidential instances boot from.
//!
//! Structurally a sibling of `crate::vprogram::manifest`: same wire idioms,
//! reusing that module's `SourceInfo` and `AttestationDescriptor` types
//! rather than redefining them.
pub mod bundle;
pub mod cmdline;
pub mod manifest;
