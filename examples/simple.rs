use async_trait::async_trait;
use pubsub::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Simple string-based topic
type SimpleTopic = String;
type SimpleKey = String;

// Simple message
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    content: String,
}

// Simple receiver
#[derive(Debug)]
struct SimpleReceiver;

#[async_trait]
impl PubSubReceiver<SimpleTopic> for SimpleReceiver {
    type Key = SimpleKey;

    async fn notify(
        &self,
        topic: SimpleTopic,
        key: Self::Key,
        payload: &[u8],
    ) -> Result<(), PubSubError> {
        let message: Message = serde_json::from_slice(payload)
            .map_err(|e| PubSubError::Mismatched(format!("Deserialization failed: {}", e)))?;

        println!(
            "Received on topic '{}' with key '{}': {}",
            topic, key, message.content
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Simple PubSub Example\n");

    // Connect to Redis
    let redis_url = "redis://127.0.0.1:6379/0".to_string();
    let pubsub = PubSubRedis::<SimpleTopic, SimpleKey>::new(redis_url).await?;

    // Register receiver
    let receiver = Arc::new(SimpleReceiver);
    pubsub.set_receiver("greetings".to_string(), receiver);

    // Start subscriber
    Arc::clone(&pubsub).run_subscriber().await?;

    println!("Subscriber started. Publishing message...\n");

    // Wait for subscriber to initialize
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Publish message
    let msg = Message {
        content: "Hello, PubSub!".to_string(),
    };

    pubsub
        .publish("greetings".to_string(), "user1".to_string(), msg)
        .await;

    // Wait for message to be received
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    println!("\nExample completed!");
    Ok(())
}
