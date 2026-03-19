# Corechan Amend Operation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `amend` action support to the corechan-operation system so operators can update metadata fields on existing CCN/CRN nodes.

**Architecture:** New `Amend` variant in `CoreChannelAction` with a typed `AmendDetails` struct (all `Option` fields, skip-serializing nulls). SDK exposes `amend_node()` following the same pattern as `link_crn`/`stake`/`drop_node`. CLI adds `node amend` subcommand with one flag per field.

**Tech Stack:** Rust, serde, clap

**Spec:** `docs/superpowers/specs/2026-03-19-corechan-amend-operation-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `crates/aleph-sdk/src/corechannel.rs` | Modify | `AmendDetails` struct, `Amend` enum variant, `amend_node` fn, tag match arm, unit test |
| `crates/aleph-cli/src/cli.rs` | Modify | `Amend(AmendNodeArgs)` variant, `AmendNodeArgs` struct |
| `crates/aleph-cli/src/commands/node.rs` | Modify | `Amend` match arm with validation and handler |

---

### Task 1: SDK — `AmendDetails` struct and `Amend` variant

**Files:**
- Modify: `crates/aleph-sdk/src/corechannel.rs`

- [ ] **Step 1: Write the failing test**

Add `test_amend_node` to the existing `mod tests` block in `crates/aleph-sdk/src/corechannel.rs`. Place it after `test_drop_node` (line ~335).

```rust
#[test]
fn test_amend_node() {
    let account = TestAccount::new();
    let node_hash = test_node_hash();
    let details = AmendDetails {
        name: Some("Updated Name".to_string()),
        reward: Some("0xNewRewardAddress".to_string()),
        ..Default::default()
    };
    let msg = amend_node(&account, node_hash, details).unwrap();

    assert_eq!(msg.message_type, MessageType::Post);

    let parsed: serde_json::Value = serde_json::from_str(&msg.item_content).unwrap();
    assert_eq!(parsed["type"], "corechan-operation");
    assert_eq!(parsed["content"]["action"], "amend");
    assert_eq!(
        parsed["content"]["tags"],
        serde_json::json!(["amend", "mainnet"])
    );
    assert_eq!(parsed["content"]["details"]["name"], "Updated Name");
    assert_eq!(
        parsed["content"]["details"]["reward"],
        "0xNewRewardAddress"
    );
    // Omitted fields must not appear (no nulls)
    assert!(parsed["content"]["details"].get("multiaddress").is_none());
    assert!(parsed["content"]["details"].get("locked").is_none());
    assert_eq!(
        parsed["ref"],
        "a75e0d10aec10614553ed00070147dd288aa4f510346cf4f5c13a826ae9f2d77"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p aleph-sdk test_amend_node`
Expected: FAIL — `AmendDetails` and `amend_node` don't exist yet.

- [ ] **Step 3: Add `AmendDetails` struct**

In `crates/aleph-sdk/src/corechannel.rs`, add after `CreateResourceNodeDetails` (line ~26):

```rust
#[derive(Debug, Default, Serialize)]
pub struct AmendDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiaddress: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub picture: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reward: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_reward: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manager: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorized: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terms_and_conditions: Option<String>,
}
```

- [ ] **Step 4: Add `Amend` variant to `CoreChannelAction`**

In the `CoreChannelAction` enum, add after `Unstake`:

```rust
    Amend { details: AmendDetails },
```

- [ ] **Step 5: Add tag match arm in `CoreChannelContent::new`**

In the `match &action` block inside `CoreChannelContent::new`, add:

```rust
            CoreChannelAction::Amend { .. } => "amend",
```

- [ ] **Step 6: Add `amend_node` public function**

After `drop_node` (line ~160), add:

```rust
pub fn amend_node<A: Account>(
    account: &A,
    node_hash: NodeHash,
    details: AmendDetails,
) -> Result<PendingMessage, MessageBuildError> {
    let action = CoreChannelAction::Amend { details };
    build_operation(account, CoreChannelContent::new(action), Some(node_hash))
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo test -p aleph-sdk test_amend_node`
Expected: PASS

- [ ] **Step 8: Run all SDK tests to check for regressions**

Run: `cargo test -p aleph-sdk`
Expected: All tests pass.

- [ ] **Step 9: Commit**

```bash
git add crates/aleph-sdk/src/corechannel.rs
git commit -m "feat(sdk): add amend action for corechan operations"
```

---

### Task 2: CLI — `AmendNodeArgs`, `NodeCommand::Amend`, and handler

**Files:**
- Modify: `crates/aleph-cli/src/cli.rs`
- Modify: `crates/aleph-cli/src/commands/node.rs`

- [ ] **Step 1: Add `Amend` variant to `NodeCommand`**

In the `NodeCommand` enum (line ~616 in `cli.rs`), add after `Drop(DropNodeArgs)`:

```rust
    /// Amend metadata fields on an existing node.
    Amend(AmendNodeArgs),
```

- [ ] **Step 2: Add `AmendNodeArgs` struct**

After `DropNodeArgs` (line ~709 in `cli.rs`), add:

```rust
#[derive(Args)]
pub struct AmendNodeArgs {
    /// Hash of the node to amend.
    #[arg(long)]
    pub node: NodeHash,

    /// Human-readable node name.
    #[arg(long)]
    pub name: Option<String>,

    /// libp2p multiaddress (CCN only).
    #[arg(long)]
    pub multiaddress: Option<String>,

    /// HTTPS endpoint address (CRN only).
    #[arg(long)]
    pub address: Option<String>,

    /// Profile picture URL.
    #[arg(long)]
    pub picture: Option<String>,

    /// Banner image URL.
    #[arg(long)]
    pub banner: Option<String>,

    /// Node description.
    #[arg(long)]
    pub description: Option<String>,

    /// Reward address.
    #[arg(long)]
    pub reward: Option<String>,

    /// PAYG stream reward address.
    #[arg(long)]
    pub stream_reward: Option<String>,

    /// Manager address.
    #[arg(long)]
    pub manager: Option<String>,

    /// Authorized staker addresses (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub authorized: Option<Vec<String>>,

    /// Restrict staking to authorized addresses.
    #[arg(long)]
    pub locked: Option<bool>,

    /// Registration URL.
    #[arg(long)]
    pub registration_url: Option<String>,

    /// Terms and conditions hash or URL.
    #[arg(long)]
    pub terms_and_conditions: Option<String>,

    #[command(flatten)]
    pub signing: SigningArgs,
}
```

- [ ] **Step 3: Add the `AmendDetails` import to `node.rs`**

At the top of `crates/aleph-cli/src/commands/node.rs`, update the `corechannel` import. Currently:

```rust
use aleph_sdk::corechannel;
```

Change to:

```rust
use aleph_sdk::corechannel::{self, AmendDetails};
```

- [ ] **Step 4: Add the `Amend` match arm in the handler**

In `handle_node_command` in `crates/aleph-cli/src/commands/node.rs`, add after the `NodeCommand::Drop` arm:

```rust
        NodeCommand::Amend(args) => {
            let details = AmendDetails {
                name: args.name,
                multiaddress: args.multiaddress,
                address: args.address,
                picture: args.picture,
                banner: args.banner,
                description: args.description,
                reward: args.reward,
                stream_reward: args.stream_reward,
                manager: args.manager,
                authorized: args.authorized,
                locked: args.locked,
                registration_url: args.registration_url,
                terms_and_conditions: args.terms_and_conditions,
            };
            if details.name.is_none()
                && details.multiaddress.is_none()
                && details.address.is_none()
                && details.picture.is_none()
                && details.banner.is_none()
                && details.description.is_none()
                && details.reward.is_none()
                && details.stream_reward.is_none()
                && details.manager.is_none()
                && details.authorized.is_none()
                && details.locked.is_none()
                && details.registration_url.is_none()
                && details.terms_and_conditions.is_none()
            {
                return Err("at least one field must be provided".into());
            }
            let account = resolve_account(&args.signing)?;
            let pending = corechannel::amend_node(&account, args.node, details)?;
            submit_or_preview(aleph_client, ccn_url, &pending, args.signing.dry_run, json).await
        }
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p aleph-cli`
Expected: Compiles with no errors or warnings.

- [ ] **Step 6: Run all tests**

Run: `cargo test --workspace`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/aleph-cli/src/cli.rs crates/aleph-cli/src/commands/node.rs
git commit -m "feat(cli): add node amend subcommand with validation"
```
