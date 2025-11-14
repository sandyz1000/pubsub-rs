use crate::error::PubSubError;
use async_trait::async_trait;
use fred::prelude::*;

#[async_trait]
pub trait PubSubPublish<Topic>: Send + Sync {
    type Key;
    async fn publish<SE: serde::Serialize + Send>(&self, topic: Topic, key: Self::Key, payload: SE);
}

#[async_trait]
pub trait PubSubReceiver<Topic>: Send + Sync + std::fmt::Debug {
    type Key: Send;

    async fn notify(&self, topic: Topic, key: Self::Key, payload: &[u8])
    -> Result<(), PubSubError>;
}

pub async fn ping_test_redis(pool: &Pool) -> Result<(), PubSubError> {
    let pong = pool.get::<String, _>("PING").await?;
    if pong != "PONG" {
        return Err(PubSubError::Mismatched(
            "redis ping failed: {pong}".to_string(),
        ));
    }

    Ok(())
}
