# Gap Analysis: Python SDK vs Rust SDK — CCN Operations

## Legend
- **P** = Python SDK, **R** = Rust SDK
- ✅ = Implemented, ❌ = Missing, ⚠️ = Partial

---

## 1. Message Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Get single message | ✅ `get_message()` | ✅ `get_message()` | Rust also returns status in `MessageWithStatus` |
| Get messages (filtered, paginated) | ✅ `get_messages()` | ✅ `get_messages()` | Both have rich filter structs |
| Get messages iterator (auto-paginate) | ✅ `get_messages_iterator()` | ✅ `get_messages_iterator()` | Both return async streams with lazy pagination |
| Watch messages (websocket) | ✅ `watch_messages()` | ✅ `subscribe_to_messages()` | Rust adds `history` param for backfill |
| Post/submit message | ✅ `submit()` | ✅ `post_message()` / `submit_message()` | Rust `submit_message` auto-uploads content to storage |
| Get message status | ✅ `get_message_status()` | ⚠️ | Rust returns status as part of `get_message`, no standalone endpoint |
| Get message error | ✅ `get_message_error()` | ❌ | Python can fetch rejection error details separately |
| Verify message signature | ❌ | ✅ `verify_message()` / `get_message_and_verify()` | Rust-only: cryptographic verification of messages |
| Generate signed message (no submit) | ✅ `generate_signed_message()` | ✅ `MessageBuilder::build()` | Both can build without sending |

## 2. Message Creation (Typed Builders)

| Message Type | Python | Rust | Notes |
|-------------|--------|------|-------|
| POST (create) | ✅ `create_post()` | ✅ `PostBuilder::new()` | |
| POST (amend) | ✅ via `ref` param | ✅ `PostBuilder::amend()` | |
| AGGREGATE | ✅ `create_aggregate()` | ✅ `AggregateBuilder::new()` | |
| STORE | ✅ `create_store()` | ✅ `StoreBuilder::new()` | |
| PROGRAM | ✅ `create_program()` | ✅ `ProgramBuilder::new()` | |
| INSTANCE | ✅ `create_instance()` | ✅ `InstanceBuilder::new()` | CLI: `aleph instance create` (credits-only, no node selection yet) |
| FORGET | ✅ `forget()` | ✅ `ForgetBuilder::new()` | |

## 3. Aggregate Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Get aggregate (typed) | ✅ `get_aggregate()` | ✅ `get_aggregate<T>()` | Rust is generic over `T: DeserializeOwned` |
| Get multiple aggregates | ✅ `get_aggregates()` (multi-key) | ✅ `get_aggregates()` | Both return a map of key → value |
| Get corechannel aggregate | ❌ (manual) | ✅ `get_corechannel_aggregate()` | Convenience in Rust |

## 4. Post Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Get posts (filtered) | ✅ `get_posts()` | ✅ `get_posts_v0()` / `get_posts_v1()` | Rust has both API versions |
| Posts iterator | ✅ `get_posts_iterator()` | ✅ `get_posts_v0_iterator()` / `get_posts_v1_iterator()` | Both auto-page; Rust has separate V0/V1 iterators |

## 5. File / Storage Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Download file (bytes) | ✅ `download_file()` | ✅ `download_file_by_hash()` | |
| Download file to path | ✅ `download_file_to_path()` | ✅ `FileDownload::to_file()` | |
| Download file to buffer | ✅ `download_file_to_buffer()` | ✅ `FileDownload::into_stream()` | Rust streams; Python writes to buffer |
| Download IPFS file | ✅ `download_file_ipfs()` | ✅ (same endpoint) | |
| Upload to storage | ✅ `storage_push()` / `storage_push_file()` | ✅ `upload_to_storage()` | |
| Upload to IPFS | ✅ `ipfs_push()` / `ipfs_push_file()` | ✅ `upload_to_ipfs()` | |
| Get file size (HEAD) | ❌ | ✅ `get_file_size()` | Rust-only |
| Get file metadata by message hash | ❌ | ✅ `get_file_metadata_by_message_hash()` | Rust-only |
| Get file metadata by ref | ❌ | ✅ `get_file_metadata_by_ref()` | Rust-only |
| Download file by ref | ❌ | ✅ `download_file_by_ref()` | Rust-only |
| Download file by message hash | ❌ | ✅ `download_file_by_message_hash()` | Rust-only |
| Get stored content metadata | ✅ `get_stored_content()` | ❌ | Python-only |
| File integrity verification on download | ❌ | ✅ `FileDownload::with_verification()` | Rust verifies hash on download |

## 6. Account / Balance Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Get balance | ✅ `get_balances()` | ✅ `get_balance()` | |
| Get total storage size | ❌ | ✅ `get_total_storage_size()` | Rust-only |
| Get VM price | ✅ `get_program_price()` / `get_estimated_price()` | ✅ `get_vm_price()` | |
| Get store estimated price | ✅ `get_store_estimated_price()` | ❌ | Python-only |
| Get credit history | ✅ `get_credit_history()` | ❌ | Python-only |

## 7. Core Channel Node Operations

| Operation | Python | Rust | Notes |
|-----------|--------|------|-------|
| Create CCN | ❌ | ✅ `create_ccn()` | Rust-only |
| Create CRN | ❌ | ✅ `create_crn()` | Rust-only |
| Link CRN to CCN | ❌ | ✅ `link_crn()` | Rust-only |
| Unlink CRN from CCN | ❌ | ✅ `unlink_crn()` | Rust-only |
| Stake on node | ❌ | ✅ `stake()` | Rust-only |
| Unstake from node | ❌ | ✅ `unstake()` | Rust-only |
| Drop node | ❌ | ✅ `drop_node()` | Rust-only |

## 8. Services (Python-only — high-level service layer)

| Service | Python | Rust | Notes |
|---------|--------|------|-------|
| CRN service (list CRNs, get VMs, GPU discovery) | ✅ | ❌ | Major feature set |
| Instance service (allocations, executions) | ✅ | ❌ | |
| Pricing service (pricing aggregate, per-service pricing) | ✅ | ❌ | |
| DNS service (public DNS, instance DNS) | ✅ | ❌ | |
| Scheduler service (plan, nodes, allocation) | ✅ | ❌ | |
| Voucher service (Solana/EVM vouchers) | ✅ | ❌ | |
| Network settings service | ✅ | ❌ | |
| Port forwarder service | ✅ | ❌ | |
| Authorization management | ✅ | ❌ | `add_authorization`, `revoke_all_authorizations`, etc. |

---

## Summary of Critical Gaps

### Rust missing (high priority for parity)

1. ~~**StoreBuilder**~~ — ✅ Implemented
2. ~~**ProgramBuilder**~~ — ✅ Implemented
3. ~~**InstanceBuilder**~~ — ✅ Implemented (CLI: credits-only, no node selection yet)
4. ~~**Auto-paginating iterators**~~ — ✅ Implemented
5. ~~**Multi-key aggregate fetch**~~ — ✅ Implemented
6. **Message error details** — `get_message_error()`
7. **Credit history** — `get_credit_history()`
8. **Store estimated price** — `get_store_estimated_price()`
9. **Authorization management** — add/revoke authorizations via security aggregate

### Rust missing (lower priority — service layer)

10. CRN service (listing, filtering, GPU discovery)
11. Instance service (allocation tracking)
12. Pricing / DNS / Scheduler / Voucher / Port forwarder services

### Rust strengths (Python doesn't have)

- Message signature verification (`verify_message`, `get_message_and_verify`)
- File metadata endpoints (`get_file_metadata_by_ref`, `get_file_metadata_by_message_hash`)
- Download integrity verification (`FileDownload::with_verification()`)
- CCN/CRN node management operations (create, link, stake, drop)
- Retry configuration with exponential backoff
- Concurrent request limiting
