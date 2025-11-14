use crate::{
    core::{PubSubPublish, PubSubReceiver},
    error::PubSubError,
};
use async_trait::async_trait;
use dashmap::DashMap;
use fred::prelude::*;
use log::{debug, error, info, warn};
use std::str::FromStr;
use std::{fmt::Debug, hash::Hash, sync::Arc, time::Duration};

pub trait Topic: Send + 'static + Sync + Hash + Eq + FromStr + Clone + Debug {}

// Provide a blanket implementation of type
impl<K> Topic for K where K: Send + 'static + Sync + Hash + Eq + FromStr + Clone + Debug {}

pub struct PubSubRedis<Topic: Debug, K> {
    receivers: Arc<DashMap<Topic, Arc<dyn PubSubReceiver<Topic, Key = K>>>>,
    redis: Client,
    pool: Option<Pool>,
    topic_prefix: String,
    retry_delay_secs: u64,
}

impl<K, T> PubSubRedis<T, K>
where
    K: Send + Sync + 'static + FromStr + Clone + Hash + Eq,
    T: Topic,
    <T as FromStr>::Err: Debug,
    <K as FromStr>::Err: Debug,
{
    /// Create a connection pool
    pub async fn create_pool(url: &str, pool_size: usize) -> Result<Pool, PubSubError> {
        let redis_config = Config::from_url(url).map_err(|e| {
            error!("Failed to create redis config from url: {}", e);
            e
        })?;
        let pool = Builder::from_config(redis_config).build_pool(pool_size)?;
        pool.init().await?;

        Ok(pool)
    }

    /// Get the client from the pool
    pub fn get_client(pool: &Pool) -> Client {
        pool.next_connected().clone()
    }

    /// Create a new PubSubRedis instance with default settings
    /// url format: "redis://localhost:6379/1"
    pub async fn new(url: String) -> Result<Arc<Self>, PubSubError> {
        Self::with_options(url, None, None).await
    }

    /// Create a new PubSubRedis instance with custom options
    pub async fn with_options(
        url: String,
        topic_prefix: Option<String>,
        retry_delay_secs: Option<u64>,
    ) -> Result<Arc<Self>, PubSubError> {
        let redis_config = Config::from_url(&url)?;
        let redis = Builder::from_config(redis_config)
            .with_connection_config(|conn_config| {
                conn_config.connection_timeout = Duration::from_secs(5);
                conn_config.tcp = TcpConfig {
                    nodelay: Some(true),
                    ..Default::default()
                };
            })
            .build()?;
        redis.init().await?;

        redis.on_error(|(error, server)| async move {
            error!("Redis connection error on {:?}: {:?}", server, error);
            Ok(())
        });

        let new = Self {
            redis,
            receivers: Arc::new(DashMap::new()),
            pool: None,
            topic_prefix: topic_prefix.unwrap_or_else(|| "la".to_string()),
            retry_delay_secs: retry_delay_secs.unwrap_or(2),
        };

        Ok(Arc::new(new))
    }

    /// Create a new PubSubRedis instance with a connection pool
    pub async fn with_pool(
        pool: Pool,
        topic_prefix: Option<String>,
    ) -> Result<Arc<Self>, PubSubError> {
        let redis = pool.next_connected().clone();

        redis.on_error(|(error, server)| async move {
            error!("Redis connection error on {:?}: {:?}", server, error);
            Ok(())
        });

        let new = Self {
            redis,
            receivers: Arc::new(DashMap::new()),
            pool: Some(pool),
            topic_prefix: topic_prefix.unwrap_or_else(|| "la".to_string()),
            retry_delay_secs: 2,
        };

        Ok(Arc::new(new))
    }

    /// Set custom topic prefix (default: "la")
    pub fn set_topic_prefix(&mut self, prefix: String) {
        self.topic_prefix = prefix;
    }

    /// Set retry delay in seconds (default: 2)
    pub fn set_retry_delay(&mut self, delay_secs: u64) {
        self.retry_delay_secs = delay_secs;
    }

    /// Register a receiver for a topic
    pub fn set_receiver(&self, topic: T, receiver: Arc<dyn PubSubReceiver<T, Key = K>>) {
        self.receivers.insert(topic, receiver);
    }

    /// Unsubscribe from a topic by removing its receiver
    pub fn unsubscribe(&self, topic: &T) -> Option<Arc<dyn PubSubReceiver<T, Key = K>>> {
        self.receivers.remove(topic).map(|(_, v)| v)
    }

    /// Get the number of registered receivers
    pub fn receiver_count(&self) -> usize {
        self.receivers.len()
    }

    /// Clear all receivers
    pub fn clear_receivers(&self) {
        self.receivers.clear();
    }

    /// Run the subscriber in the background
    pub async fn run_subscriber(self: Arc<PubSubRedis<T, K>>) -> Result<(), PubSubError> {
        let sub = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = sub.subscriber_task().await {
                    error!(
                        "Subscriber task failed: {}. Retrying in {} seconds...",
                        e, sub.retry_delay_secs
                    );
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(sub.retry_delay_secs)).await;
            }
        });

        Ok(())
    }

    async fn subscriber_task(self: &Arc<Self>) -> Result<(), PubSubError> {
        info!("Starting subscriber task");

        let subscriber_client = if let Some(ref pool) = self.pool {
            pool.next_connected().clone()
        } else {
            self.redis.clone_new()
        };

        subscriber_client.init().await?;
        info!("Subscriber client initialized");
        let pubsub = self.clone();
        let topic_prefix = self.topic_prefix.clone();

        // Subscribe to all topics matching our prefix pattern
        let pattern = format!("{}/*", topic_prefix);
        info!("Subscribing to pattern: {}", pattern);
        subscriber_client.psubscribe(&pattern).await?;
        info!("Successfully subscribed to pattern: {}", pattern);

        // Run the task in the background
        let message_task = subscriber_client.on_message(move |message| {
            let pubsub = Arc::clone(&pubsub);
            let prefix = topic_prefix.clone();
            async move {
                let payload = message.value.convert::<Vec<u8>>()?;
                let channel = message.channel;

                debug!(
                    "Received message: {} bytes on topic {}",
                    payload.len(),
                    channel
                );

                let prefix_pattern = format!("{}/", prefix);
                if let Some(pair) = channel.strip_prefix(&prefix_pattern) {
                    let parts: Vec<&str> = pair.split('/').collect();
                    if parts.len() >= 2 {
                        match (parts[0].parse(), parts[1].parse()) {
                            (Ok(topic), Ok(key)) => {
                                pubsub.forward_to_receiver(key, topic, payload);
                            }
                            (Err(e), _) => {
                                warn!("Failed to parse topic from '{}': {:?}", parts[0], e);
                            }
                            (_, Err(e)) => {
                                warn!("Failed to parse key from '{}': {:?}", parts[1], e);
                            }
                        }
                    } else {
                        warn!("Invalid channel format: {}", channel);
                    }
                } else {
                    debug!("Ignoring message on non-matching channel: {}", channel);
                }
                Ok::<_, Error>(())
            }
        });
        let _ = message_task.await;

        Ok(())
    }

    fn forward_to_receiver(self: &Arc<Self>, key: K, topic: T, payload: Vec<u8>) {
        let receivers = Arc::clone(&self.receivers);
        tokio::spawn(async move {
            let payload = payload.as_ref();

            if let Some(receiver) = receivers.get(&topic) {
                if let Err(e) = receiver.notify(topic, key, payload).await {
                    error!("Failed to notify receiver: {:?}", e);
                }
            } else {
                warn!("No receiver registered for topic: {:?}", topic);
            }
        });
    }
}

#[async_trait]
impl<T, Topic> PubSubPublish<Topic> for PubSubRedis<Topic, T>
where
    T: Send + 'static + std::fmt::Debug,
    Topic: std::fmt::Debug + Send + Sync,
{
    type Key = T;

    async fn publish<SE: serde::Serialize + Send>(
        &self,
        topic: Topic,
        key: Self::Key,
        payload: SE,
    ) {
        match serde_json::to_vec(&payload) {
            Ok(bytes) => {
                let channel = format!("{}/{:?}/{:?}", self.topic_prefix, topic, key);

                // Use pool if available for better load distribution
                let client = if let Some(ref pool) = self.pool {
                    pool.next_connected()
                } else {
                    &self.redis
                };

                if let Err(e) = client.publish::<(), String, &[u8]>(channel, &bytes).await {
                    error!("Failed to publish message: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to serialize payload: {}", e);
            }
        };
    }
}
