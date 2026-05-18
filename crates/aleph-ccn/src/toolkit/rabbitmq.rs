//! RabbitMQ connection helpers. Mirrors `aleph/toolkit/rabbitmq.py` plus the
//! ad-hoc `declare_exchange` calls scattered across `aleph/network.py`,
//! `aleph/chains/chain_data_service.py`, and `aleph/jobs/*`.
//!
//! Python uses `aio_pika.connect_robust(...)` and `channel.declare_exchange(...)`.
//! Here we use `lapin` 2.x — the equivalent calls are
//! [`lapin::Connection::connect`] and [`lapin::Channel::exchange_declare`].

use lapin::options::ExchangeDeclareOptions;
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, ExchangeKind};

use crate::config::{P2pSettings, RabbitmqSettings};
use crate::{AlephError, AlephResult};

fn amqp_uri(host: &str, port: u16, username: &str, password: &str) -> String {
    format!("amqp://{}:{}@{}:{}/%2f", username, password, host, port)
}

/// Build an AMQP URI from `RabbitmqSettings` alone.
pub fn rabbitmq_uri(rabbitmq: &RabbitmqSettings) -> String {
    amqp_uri(
        &rabbitmq.host,
        rabbitmq.port,
        &rabbitmq.username,
        &rabbitmq.password,
    )
}

/// Build the URI used by `aleph/toolkit/rabbitmq.py::make_mq_conn`, which
/// dials `config.p2p.mq_host` rather than `config.rabbitmq.host`.
pub fn rabbitmq_p2p_uri(p2p: &P2pSettings, rabbitmq: &RabbitmqSettings) -> String {
    amqp_uri(
        &p2p.mq_host,
        rabbitmq.port,
        &rabbitmq.username,
        &rabbitmq.password,
    )
}

/// Mirrors `aleph.toolkit.rabbitmq.make_mq_conn`. Connects to the broker
/// located at `p2p.mq_host`.
///
/// `lapin` does not have a separate `connect_robust`; it provides automatic
/// reconnection through the standard `Connection` object plus error events.
pub async fn make_mq_conn(
    p2p: &P2pSettings,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<Connection> {
    let uri = rabbitmq_p2p_uri(p2p, rabbitmq);
    Connection::connect(&uri, ConnectionProperties::default())
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq connect failed: {e}")))
}

/// Connect to the broker located at `rabbitmq.host` (used by job processors
/// that don't go through the p2p container).
pub async fn make_mq_conn_direct(rabbitmq: &RabbitmqSettings) -> AlephResult<Connection> {
    let uri = rabbitmq_uri(rabbitmq);
    Connection::connect(&uri, ConnectionProperties::default())
        .await
        .map_err(|e| AlephError::P2p(format!("rabbitmq connect failed: {e}")))
}

/// Declare a topic exchange. Mirrors the
/// `channel.declare_exchange(name, type=ExchangeType.TOPIC, auto_delete=False)`
/// idiom used across pyaleph.
pub async fn declare_topic_exchange(channel: &Channel, name: &str) -> AlephResult<()> {
    channel
        .exchange_declare(
            name,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                passive: false,
                // pyaleph uses aio_pika's default `durable=False`. Two CCNs
                // co-talking would otherwise mismatch and fail PRECONDITION_FAILED.
                durable: false,
                auto_delete: false,
                internal: false,
                nowait: false,
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| AlephError::P2p(format!("exchange_declare {name} failed: {e}")))?;
    Ok(())
}

/// Declare the `pending_message_exchange` topic exchange.
///
/// Mirrors the `pending_message_exchange = await channel.declare_exchange(
///     name=config.rabbitmq.pending_message_exchange.value, ...)` calls in
/// `aleph/network.py` and `aleph/jobs/*`.
pub async fn declare_pending_message_exchange(
    channel: &Channel,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<()> {
    declare_topic_exchange(channel, &rabbitmq.pending_message_exchange).await
}

/// Declare the `pending_tx_exchange` topic exchange. Mirrors
/// `make_pending_tx_exchange` in `aleph/chains/chain_data_service.py`.
pub async fn declare_pending_tx_exchange(
    channel: &Channel,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<()> {
    declare_topic_exchange(channel, &rabbitmq.pending_tx_exchange).await
}

/// Declare the `message_exchange` topic exchange (mirrors
/// `mq_message_exchange = await channel.declare_exchange(...)`).
pub async fn declare_message_exchange(
    channel: &Channel,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<()> {
    declare_topic_exchange(channel, &rabbitmq.message_exchange).await
}

/// Declare the `pub_exchange` topic exchange (used by `aleph_p2p_client` to
/// publish to other peers).
pub async fn declare_pub_exchange(
    channel: &Channel,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<()> {
    declare_topic_exchange(channel, &rabbitmq.pub_exchange).await
}

/// Declare the `sub_exchange` topic exchange (used by `aleph_p2p_client` to
/// subscribe to other peers).
pub async fn declare_sub_exchange(
    channel: &Channel,
    rabbitmq: &RabbitmqSettings,
) -> AlephResult<()> {
    declare_topic_exchange(channel, &rabbitmq.sub_exchange).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rmq_settings() -> RabbitmqSettings {
        RabbitmqSettings {
            host: "rabbitmq".into(),
            port: 5672,
            username: "guest".into(),
            password: "guest".into(),
            pub_exchange: "p2p-publish".into(),
            sub_exchange: "p2p-subscribe".into(),
            message_exchange: "aleph-messages".into(),
            pending_message_exchange: "aleph-pending-messages".into(),
            pending_tx_exchange: "aleph-pending-txs".into(),
            heartbeat: 600,
        }
    }

    fn p2p_settings() -> P2pSettings {
        let mut s = P2pSettings::default();
        s.mq_host = "p2p-service".into();
        s
    }

    #[test]
    fn rabbitmq_uri_direct() {
        let s = rmq_settings();
        let uri = rabbitmq_uri(&s);
        assert_eq!(uri, "amqp://guest:guest@rabbitmq:5672/%2f");
    }

    #[test]
    fn rabbitmq_uri_via_p2p_host() {
        let r = rmq_settings();
        let p = p2p_settings();
        let uri = rabbitmq_p2p_uri(&p, &r);
        assert_eq!(uri, "amqp://guest:guest@p2p-service:5672/%2f");
    }

    #[tokio::test]
    async fn make_mq_conn_fails_on_unreachable_host() {
        // Use a port we know to be closed to assert the error path is wired.
        let r = RabbitmqSettings {
            host: "127.0.0.1".into(),
            port: 1,
            username: "x".into(),
            password: "x".into(),
            ..rmq_settings()
        };
        let p = P2pSettings {
            mq_host: "127.0.0.1".into(),
            ..P2pSettings::default()
        };
        let err = make_mq_conn(&p, &r).await.unwrap_err();
        assert!(matches!(err, AlephError::P2p(_)));
    }

    #[tokio::test]
    async fn make_mq_conn_direct_fails_on_unreachable_host() {
        let r = RabbitmqSettings {
            host: "127.0.0.1".into(),
            port: 1,
            username: "x".into(),
            password: "x".into(),
            ..rmq_settings()
        };
        let err = make_mq_conn_direct(&r).await.unwrap_err();
        assert!(matches!(err, AlephError::P2p(_)));
    }
}
