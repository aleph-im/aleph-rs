//! Client-side pipeline for creating V-Programs (verifiable SEV-SNP programs).
//!
//! Protocol: aleph-vm docs/plans/2026-07-08-confidential-vm-protocol-design.md
//!
//! `manifest` and `cmdline` are plain serde/string types with no dependency
//! beyond serde, and they describe the wire format rather than any part of
//! the measurement pipeline. They are therefore always available, so that
//! consumers which only need to *read* a runtime manifest (the scheduler
//! sizing a v-program's platform bundle, for one) do not have to pull in the
//! attestation crypto stack behind the `vprogram` feature.
//!
//! Everything that actually computes or verifies a measurement stays gated.
#[cfg(feature = "vprogram")]
pub mod bundle;
pub mod cmdline;
pub mod manifest;
#[cfg(feature = "vprogram")]
pub mod measure;
#[cfg(feature = "vprogram")]
pub mod status;
