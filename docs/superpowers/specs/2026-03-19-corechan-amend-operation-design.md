# Corechan Amend Operation

## Summary

Add support for the `amend` action on corechan-operation POST messages, allowing
operators to update metadata fields on existing CCN/CRN nodes.

## Message Structure

A standard corechan-operation POST with `action: "amend"`, a `ref` pointing to
the target node hash, and a partial `details` object containing only the fields
being changed:

```json
{
  "type": "corechan-operation",
  "ref": "<node_hash>",
  "content": {
    "action": "amend",
    "tags": ["amend", "mainnet"],
    "details": {
      "name": "Updated Name"
    }
  }
}
```

## SDK Changes (`aleph-sdk/src/corechannel.rs`)

### `AmendDetails` struct

All 13 editable fields as `Option<T>`, with `#[serde(skip_serializing_if = "Option::is_none")]`
so only provided fields appear in the serialized JSON. Derives `Debug, Default, Serialize`.

Field names use serde's default (underscore) serialization, which matches the
wire protocol (confirmed via `aggregate.json` and `instance-gpu-payg.json` fixtures).
No `#[serde(rename)]` attributes needed.

Fields:

| Field                  | Type                   |
|------------------------|------------------------|
| `name`                 | `Option<String>`       |
| `multiaddress`         | `Option<String>`       |
| `address`              | `Option<String>`       |
| `picture`              | `Option<String>`       |
| `banner`               | `Option<String>`       |
| `description`          | `Option<String>`       |
| `reward`               | `Option<String>`       |
| `stream_reward`        | `Option<String>`       |
| `manager`              | `Option<String>`       |
| `authorized`           | `Option<Vec<String>>`  |
| `locked`               | `Option<bool>`         |
| `registration_url`     | `Option<String>`       |
| `terms_and_conditions` | `Option<String>`       |

### `CoreChannelAction` enum

New variant: `Amend { details: AmendDetails }`. Serializes as `"action": "amend"`
via the existing `#[serde(tag = "action", rename_all = "kebab-case")]`.

New tag entry in `CoreChannelContent::new`: `"amend"`.

### `amend_node` function

```rust
pub fn amend_node<A: Account>(
    account: &A,
    node_hash: NodeHash,
    details: AmendDetails,
) -> Result<PendingMessage, MessageBuildError>
```

Calls `build_operation` with `Some(node_hash)` as reference — same pattern as
`link_crn`, `stake`, `drop_node`, etc.

## CLI Changes (`aleph-cli`)

### `NodeCommand` enum (`cli.rs`)

New variant: `Amend(AmendNodeArgs)`.

### `AmendNodeArgs` struct (`cli.rs`)

```
--node <NodeHash>              (required) Hash of the node to amend
--name <String>                (optional)
--multiaddress <String>        (optional)
--address <String>             (optional)
--picture <String>             (optional)
--banner <String>              (optional)
--description <String>         (optional)
--reward <String>              (optional)
--stream-reward <String>       (optional)
--manager <String>             (optional)
--authorized <String>...       (optional, comma-delimited via value_delimiter = ',')
--locked <bool>                (optional)
--registration-url <String>    (optional)
--terms-and-conditions <String>(optional)
```

Plus the standard `#[command(flatten)] signing: SigningArgs`.

### Handler (`commands/node.rs`)

New match arm: maps `AmendNodeArgs` fields into `AmendDetails`, calls
`corechannel::amend_node`, then `submit_or_preview`.

**Validation:** If all 13 optional fields are `None`, error early with
"at least one field must be provided" rather than submitting a no-op. This
check lives in the CLI handler, not the SDK — the SDK is a faithful message
builder and permits empty details (the server would reject it).

## Tests

### SDK unit test (`corechannel.rs`)

`test_amend_node`: builds a message with a subset of fields set (e.g. `name`
and `reward`), asserts:

- `action` is `"amend"`
- `tags` are `["amend", "mainnet"]`
- `ref` is the node hash
- `details` contains only the set fields (no null values for omitted fields)

Follows the existing test pattern (`test_create_ccn`, `test_link_crn`, etc.).

## Files Modified

| File | Change |
|------|--------|
| `crates/aleph-sdk/src/corechannel.rs` | `AmendDetails` struct, `Amend` variant, `amend_node` fn, tag match arm, unit test |
| `crates/aleph-cli/src/cli.rs` | `Amend(AmendNodeArgs)` variant, `AmendNodeArgs` struct |
| `crates/aleph-cli/src/commands/node.rs` | `Amend` match arm with validation and handler |
