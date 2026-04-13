# Authenticated File Upload

## Problem

The `POST /api/v0/storage/add_file` endpoint in heph accepts file uploads without
authentication. All uploads are treated the same regardless of whether the caller
provides signed metadata. The existing multipart metadata path fires off message
processing best-effort but does not gate file storage on signature validity, file
hash integrity, or user balance.

This means:
- Large file uploads cannot be distinguished from spam.
- There is no way to enforce per-user size limits based on credit balance.
- The SDK performs file upload and STORE message submission as two separate HTTP
  calls, which is fragile (upload succeeds but message post fails).

## Goal

Make heph enforce the same authenticated upload protocol as pyaleph: when a signed
STORE message is provided as multipart metadata, verify the signature, check the
file hash matches, check the user's balance, and only then store the file. Update
the SDK to always send metadata for STORE file uploads, combining upload and message
processing into a single request.

## Scope

- **In scope:** Storage uploads (`/api/v0/storage/add_file`) on both server and SDK.
- **Out of scope:** IPFS uploads (`/api/v0/ipfs/add_file`) — no metadata support
  for now.

## Design

### Server (heph): `add_file` / `add_file_multipart`

File: `crates/heph/src/api/storage.rs`

#### Metadata schema

The `metadata` multipart field is a JSON object:

```json
{
  "message": { <IncomingMessage — a signed STORE message> },
  "sync": false
}
```

The `message` field is an `IncomingMessage` with `type: "STORE"` and
`item_type: "inline"`. Its `item_content` is a JSON string containing
`StoreContent`, which includes the `item_hash` of the file being uploaded.

Define a `StorageMetadata` struct:

```rust
#[derive(Deserialize)]
struct StorageMetadata {
    message: IncomingMessage,
    sync: bool,
}
```

#### Upload flow with metadata

When the `metadata` multipart field is present:

1. **Parse metadata** — deserialize as `StorageMetadata`. Return 422 if invalid.
2. **Validate message type** — `message.message_type` must be `STORE` and
   `message.item_type` must be `Inline`. Return 422 otherwise.
3. **Verify signature** — call `verify_signature(&message)`. Return 403 if invalid.
4. **Parse store content** — deserialize `message.item_content` as `StoreContent`.
   Return 422 if invalid or if `item_content` is missing.
5. **Check file hash** — the SHA-256 hash of the uploaded file bytes must match
   `store_content.file_hash.item_hash`. Return 422 if mismatch.
6. **Check balance** — call `check_balance()` with the message and content.
   Return 402 if insufficient.
7. **Store file** — write to `FileStore`. Insert into `files` DB table.
8. **Process message** — call `process_message_with_store()` to insert the STORE
   message and create file pins, cost records, etc. Note: this re-runs signature
   and balance checks internally, which is harmless (idempotent). The pre-checks
   in steps 3-6 are needed to gate the file write; the re-checks inside the
   processing pipeline are a side effect of reusing the full pipeline.
9. **Return** `{"status": "success", "hash": "<sha256>"}` with 200.

#### Upload flow without metadata

When the `metadata` field is absent (unauthenticated upload):

1. Enforce `MAX_SIZE_NO_META` (25 MiB) limit.
2. Store file to `FileStore`.
3. Insert into `files` DB table with a grace period.
4. Return `{"status": "success", "hash": "<sha256>"}`.

No signature verification, no balance check. This path exists to support
non-inline message content uploads (via `submit_message`).

#### Size limits

- Without metadata: 25 MiB (`MAX_SIZE_NO_META`).
- With metadata: 100 MiB (`MAX_SIZE_WITH_META`).

The size check must happen after metadata is parsed, since the limit depends on
whether metadata is present. The current code already does this.

#### Raw binary uploads (non-multipart)

Raw `application/octet-stream` uploads remain unauthenticated with the 25 MiB
limit. These are used by `submit_message` for non-inline content.

#### Error responses

| Condition | HTTP Status | Body |
|-----------|------------|------|
| Invalid metadata JSON | 422 | `{"error": "Could not decode metadata: ..."}` |
| Message type not STORE | 422 | `{"error": "Metadata message must be a STORE message"}` |
| Missing item_content | 422 | `{"error": "Store message content needed"}` |
| Invalid store content | 422 | `{"error": "Invalid store message content: ..."}` |
| File hash mismatch | 422 | `{"error": "File hash does not match (X != Y)"}` |
| Invalid signature | 403 | `{"error": "Invalid signature"}` |
| Insufficient balance | 402 | `{"error": "Insufficient balance"}` |
| File too large | 413 | `{"error": "File too large"}` |

### SDK: `AlephStorageClient` trait

File: `crates/aleph-sdk/src/client.rs`

#### Trait changes

Add an optional `PendingMessage` parameter to the storage upload methods:

```rust
fn upload_to_storage(
    &self,
    data: &[u8],
    message: Option<&PendingMessage>,
) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;

fn upload_file_to_storage(
    &self,
    path: impl AsRef<std::path::Path> + Send,
    message: Option<&PendingMessage>,
) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;
```

IPFS methods are unchanged.

#### Metadata construction

When `message` is `Some`, serialize a `StorageMetadata` JSON object and attach it
as a `metadata` text part in the multipart form:

```rust
let meta = serde_json::json!({
    "message": message,
    "sync": false
});
let form = form.part(
    "metadata",
    Part::text(meta.to_string()).mime_str("application/json")
);
```

The `sync` field is hardcoded to `false` — synchronous processing is not needed
since the server processes the message inline during the upload request.

#### `submit_message` changes

`submit_message` calls `upload_to_storage` for non-inline content uploads. It
should pass `None` as the message parameter (unauthenticated upload of message
content, not user files).

#### `create_store` changes

`create_store` currently does:
1. Upload file (no auth)
2. Build STORE message
3. Post message separately

New flow:
1. Hash file locally (already done in `upload_file_to_storage` pass 1)
2. Build and sign STORE message using the local hash
3. Upload file + metadata in one call via `upload_file_to_storage(path, Some(&message))`
4. No separate `post_message` call — the server processes the message during upload

`create_store` needs the account parameter it already has. It will use
`StoreBuilder` to build the message before uploading.

The return type changes slightly: `create_store` currently returns
`PostMessageResponse` (from `post_message`). Since the server now processes the
message during upload, the upload response only contains the file hash. We can
either:
- Have `create_store` return just `ItemHash` (simpler, breaking change).
- Construct a `PostMessageResponse` from the upload result (backward compat).

Decision: return `ItemHash`. The `PostMessageResponse` mainly carries
`publication_status` which is a CCN concept not relevant to heph.

### CLI changes

File: `crates/aleph-cli/src/commands/file.rs`

The CLI currently does the same two-step flow as `create_store`. It should switch
to a single upload call with metadata:

1. Hash file locally.
2. Build STORE message with `StoreBuilder` (reference, channel, payment, etc.).
3. If `--dry-run`: display the message preview and stop (no upload).
4. Call `upload_file_to_storage(path, Some(&message))`.
5. Done — no separate message submission.

The CLI already has access to the account and all the builder options, so
constructing the message first is straightforward. The file hash is needed before
building the message, so we need a way to hash without uploading. The existing
`hash_file` utility (used internally by `upload_file_to_storage`) can be made
public, or a dedicated `compute_file_hash` method can be added to the trait.

Simplest approach: make the existing `hash_file` utility public (it currently
lives in the client module). It takes a path and a `Hasher` and returns an
`ItemHash`. No trait method needed — it's a pure local computation with no
network dependency.

### Data flow summary

```
CLI / SDK user
    |
    v
hash_file(path, Hasher::for_storage()) --> ItemHash (local SHA-256)
    |
    v
StoreBuilder::new(account, file_hash, engine).build() --> PendingMessage
    |
    v
upload_file_to_storage(path, Some(&message))
    |
    |  multipart: { file: <bytes>, metadata: { message: ..., sync: false } }
    v
heph server
    |
    +-- parse metadata
    +-- verify signature (403)
    +-- parse store content, check file hash (422)
    +-- check balance (402)
    +-- store file
    +-- process_message_with_store()
    +-- return { status: "success", hash: "..." }
```

## Testing

### Server tests (heph)

- **Authenticated upload succeeds**: upload file with valid signed metadata, verify
  file is stored and STORE message is processed (check DB for message + file pin).
- **Signature rejection**: upload with tampered signature, verify 403 and file is
  NOT stored.
- **Hash mismatch rejection**: upload file with metadata whose store content
  `item_hash` doesn't match, verify 422 and file is NOT stored.
- **Balance rejection**: upload with valid signature but insufficient balance,
  verify 402 and file is NOT stored.
- **Unauthenticated upload still works**: upload without metadata within 25 MiB
  limit, verify success.
- **Unauthenticated upload rejected over limit**: upload without metadata over
  25 MiB, verify 413.
- **Authenticated upload allows larger files**: upload with metadata over 25 MiB
  but under 100 MiB, verify success.

### SDK tests

- **Upload with metadata**: verify the multipart form includes both `file` and
  `metadata` fields, and the metadata JSON is well-formed.
- **Upload without metadata**: verify `submit_message` path sends file only.
- **`create_store` integration**: verify end-to-end flow against a running heph
  instance (existing integration test pattern).
