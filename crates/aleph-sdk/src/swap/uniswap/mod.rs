//! Uniswap V3 provider.
//!
//! Quotes on-chain through QuoterV2 (`eth_call`, no API key) and executes
//! through SwapRouter02. Unlike CoW, swaps execute immediately at the pool
//! price and the caller pays gas; the pool fee is embedded in the quoted
//! price rather than charged separately.

pub mod chains;
