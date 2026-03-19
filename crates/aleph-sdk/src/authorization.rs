use std::future::Future;

use aleph_types::account::Account;
use aleph_types::chain::Address;
use aleph_types::message::{Authorization, SecurityAggregateContent};

use crate::aggregate_models::security::SecurityAggregate;
use crate::client::{
    AlephAggregateClient, AlephMessageClient, AlephStorageClient, MessageError, PostMessageResponse,
};
use crate::messages::AggregateBuilder;

/// Trait for reading authorization data from the Aleph network.
pub trait AlephAuthorizationClient: AlephAggregateClient {
    /// Fetch all authorizations for an address.
    /// Returns empty vec if no security aggregate exists.
    fn get_authorizations(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<Vec<Authorization>, MessageError>> + Send
    where
        Self: Sync,
    {
        async move {
            match self
                .get_aggregate::<SecurityAggregate>(address, "security")
                .await
            {
                Ok(agg) => Ok(agg.security.authorizations),
                // A missing security aggregate causes a deserialization failure
                // (the response body doesn't match SecurityAggregate), which
                // surfaces as HttpError. Treat that as "no authorizations yet".
                // Other error variants (ApiError, Io, etc.) are propagated.
                Err(MessageError::HttpError(_)) => Ok(vec![]),
                Err(other) => Err(other),
            }
        }
    }
}

impl AlephAuthorizationClient for crate::client::AlephClient {}

/// Replace all authorizations for the account.
/// Builds an AGGREGATE message with key "security" and submits it.
pub async fn update_all_authorizations<A, C>(
    client: &C,
    account: &A,
    authorizations: Vec<Authorization>,
) -> Result<PostMessageResponse, MessageError>
where
    A: Account,
    C: AlephMessageClient + AlephStorageClient + Sync,
{
    let content = SecurityAggregateContent { authorizations };
    let content_map =
        match serde_json::to_value(&content).map_err(crate::messages::MessageBuildError::from)? {
            serde_json::Value::Object(map) => map,
            _ => unreachable!("SecurityAggregateContent always serializes to an object"),
        };
    let message = AggregateBuilder::new(account, "security", content_map).build()?;
    client.submit_message(&message, true).await
}

/// Add a single authorization, preserving existing ones.
/// Fetches existing authorizations, appends the new one, and submits.
pub async fn add_authorization<A, C>(
    client: &C,
    account: &A,
    authorization: Authorization,
) -> Result<PostMessageResponse, MessageError>
where
    A: Account,
    C: AlephMessageClient + AlephAuthorizationClient + AlephStorageClient + Sync,
{
    let mut authorizations = client.get_authorizations(account.address()).await?;
    authorizations.push(authorization);
    update_all_authorizations(client, account, authorizations).await
}

/// Remove all authorizations for a specific delegate address.
/// Fetches existing authorizations, filters out the delegate, and submits.
pub async fn revoke_all_authorizations<A, C>(
    client: &C,
    account: &A,
    delegate_address: &Address,
) -> Result<PostMessageResponse, MessageError>
where
    A: Account,
    C: AlephMessageClient + AlephAuthorizationClient + AlephStorageClient + Sync,
{
    let authorizations = client
        .get_authorizations(account.address())
        .await?
        .into_iter()
        .filter(|auth| auth.address != *delegate_address)
        .collect();
    update_all_authorizations(client, account, authorizations).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AlephClient;
    use aleph_types::account::{Account, EvmAccount};
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;
    use url::Url;

    fn heph_client() -> AlephClient {
        AlephClient::new(Url::parse("http://localhost:4024").expect("valid url"))
    }

    fn test_account(key_byte: u8) -> EvmAccount {
        EvmAccount::new(Chain::Ethereum, &[key_byte; 32]).expect("valid key")
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_get_authorizations_empty() {
        let client = heph_client();
        let account = test_account(200);
        let auths = client.get_authorizations(account.address()).await.unwrap();
        assert!(
            auths.is_empty(),
            "new account should have no authorizations"
        );
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_update_all_then_get() {
        let client = heph_client();
        let account = test_account(201);
        let delegate = Address::from("0xdelegate_201".to_string());

        let auths = vec![Authorization {
            address: delegate.clone(),
            chain: None,
            channels: vec![],
            types: vec![MessageType::Post],
            post_types: vec![],
            aggregate_keys: vec![],
        }];

        update_all_authorizations(&client, &account, auths)
            .await
            .unwrap();

        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].address, delegate);
        assert_eq!(fetched[0].types, vec![MessageType::Post]);
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_add_authorization_creates_aggregate() {
        let client = heph_client();
        let account = test_account(202);
        let delegate = Address::from("0xdelegate_202".to_string());

        let auth = Authorization {
            address: delegate.clone(),
            chain: Some(Chain::Ethereum),
            channels: vec![],
            types: vec![],
            post_types: vec![],
            aggregate_keys: vec![],
        };

        add_authorization(&client, &account, auth).await.unwrap();

        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].address, delegate);
        assert_eq!(fetched[0].chain, Some(Chain::Ethereum));
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_add_authorization_preserves_existing() {
        let client = heph_client();
        let account = test_account(203);
        let delegate1 = Address::from("0xdelegate_203a".to_string());
        let delegate2 = Address::from("0xdelegate_203b".to_string());

        let auth1 = Authorization {
            address: delegate1.clone(),
            chain: None,
            channels: vec![],
            types: vec![MessageType::Post],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        add_authorization(&client, &account, auth1).await.unwrap();

        let auth2 = Authorization {
            address: delegate2.clone(),
            chain: None,
            channels: vec![],
            types: vec![MessageType::Aggregate],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        add_authorization(&client, &account, auth2).await.unwrap();

        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 2);
        assert!(fetched.iter().any(|a| a.address == delegate1));
        assert!(fetched.iter().any(|a| a.address == delegate2));
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_revoke_nonexistent_delegate_is_noop() {
        let client = heph_client();
        let account = test_account(204);
        let delegate = Address::from("0xdelegate_204".to_string());
        let nonexistent = Address::from("0xnonexistent".to_string());

        let auth = Authorization {
            address: delegate.clone(),
            chain: None,
            channels: vec![],
            types: vec![],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        add_authorization(&client, &account, auth).await.unwrap();

        // Revoke a delegate that doesn't exist — should not affect existing
        revoke_all_authorizations(&client, &account, &nonexistent)
            .await
            .unwrap();

        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].address, delegate);
    }

    #[tokio::test]
    #[ignore = "requires a running heph instance"]
    async fn test_full_lifecycle() {
        let client = heph_client();
        let account = test_account(205);
        let delegate1 = Address::from("0xdelegate_205a".to_string());
        let delegate2 = Address::from("0xdelegate_205b".to_string());

        // Add two authorizations
        let auth1 = Authorization {
            address: delegate1.clone(),
            chain: None,
            channels: vec!["ch1".to_string()],
            types: vec![MessageType::Post],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        add_authorization(&client, &account, auth1).await.unwrap();

        let auth2 = Authorization {
            address: delegate2.clone(),
            chain: None,
            channels: vec![],
            types: vec![MessageType::Program],
            post_types: vec![],
            aggregate_keys: vec![],
        };
        add_authorization(&client, &account, auth2).await.unwrap();

        // Verify both present
        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 2);

        // Revoke delegate1
        revoke_all_authorizations(&client, &account, &delegate1)
            .await
            .unwrap();

        // Verify only delegate2 remains
        let fetched = client.get_authorizations(account.address()).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].address, delegate2);
        assert_eq!(fetched[0].types, vec![MessageType::Program]);
    }
}
