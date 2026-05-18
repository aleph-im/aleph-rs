//! Wire schemas. Mirrors `aleph/schemas/`.
//!
//! Reuses `aleph_types::message` for the canonical wire format; this module
//! adds the CCN-specific query params and admin-only payloads.

pub mod addresses_query_params;
pub mod api;
pub mod base_messages;
pub mod chains;
pub mod cost_estimation_messages;
pub mod credit_transfer;
pub mod message_confirmation;
pub mod message_content;
pub mod messages_query_params;
pub mod pending_messages;
