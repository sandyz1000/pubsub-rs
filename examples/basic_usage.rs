use async_trait::async_trait;
use pubsub::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Define your message topic
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum MessageTopic {
    UserNotification,
    SystemAlert,
}

impl std::str::FromStr for MessageTopic {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "UserNotification" => Ok(MessageTopic::UserNotification),
            "SystemAlert" => Ok(MessageTopic::SystemAlert),
            _ => Err(format!("Unknown topic: {}", s)),
        }
    }
}

// Define your message key (could be user_id, session_id, etc.)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct UserId(String);

impl std::str::FromStr for UserId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(UserId(s.to_string()))
    }
}

// Define your message payload
#[derive(Debug, Serialize, Deserialize)]
struct NotificationMessage {
    title: String,
    body: String,
    timestamp: u64,
}

// Implement a receiver that handles incoming messages
#[derive(Debug)]
struct NotificationReceiver;

#[async_trait]
impl PubSubReceiver<MessageTopic> for NotificationReceiver {
    type Key = UserId;

    async fn notify(
        &self,
        topic: MessageTopic,
        key: Self::Key,
        payload: &[u8],
    ) -> Result<(), PubSubError> {
        // Deserialize the message
        let message: NotificationMessage = serde_json::from_slice(payload)
            .map_err(|e| PubSubError::Mismatched(format!("Failed to deserialize: {}", e)))?;

        println!(
            "📬 Received notification on topic {:?} for user {:?}:",
            topic, key.0
        );
        println!("Title: {}", message.title);
        println!("Body: {}", message.body);
        println!("Timestamp: {}", message.timestamp);
        println!();

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize a simple logger
    env_logger::init();

    println!("🚀 Starting PubSub Example\n");

    // Redis connection URL
    let redis_url = "redis://127.0.0.1:6379/0".to_string();

    // Create PubSub instance with default configuration
    println!("📡 Connecting to Redis at {}", redis_url);
    let pubsub = PubSubRedis::<MessageTopic, UserId>::new(redis_url.clone()).await?;

    // Register a receiver for the UserNotification topic
    let receiver = Arc::new(NotificationReceiver);
    pubsub.set_receiver(MessageTopic::UserNotification, receiver.clone());

    // Register the same receiver for SystemAlert topic
    pubsub.set_receiver(MessageTopic::SystemAlert, receiver);

    println!("✅ Receivers registered for topics");

    // Start the subscriber in the background
    let subscriber = Arc::clone(&pubsub);
    subscriber.run_subscriber().await?;

    println!("👂 Subscriber started, listening for messages...\n");

    // Give the subscriber a moment to initialize
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Publish some test messages
    println!("📤 Publishing test messages...\n");

    // Publish to UserNotification topic
    let message1 = NotificationMessage {
        title: "Welcome!".to_string(),
        body: "Thanks for joining our service".to_string(),
        timestamp: 1699900000,
    };

    pubsub
        .publish(
            MessageTopic::UserNotification,
            UserId("user123".to_string()),
            message1,
        )
        .await;

    // Wait a bit for the message to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish to SystemAlert topic
    let message2 = NotificationMessage {
        title: "System Maintenance".to_string(),
        body: "Scheduled maintenance at 2 AM UTC".to_string(),
        timestamp: 1699900100,
    };

    pubsub
        .publish(
            MessageTopic::SystemAlert,
            UserId("user123".to_string()),
            message2,
        )
        .await;

    // Wait a bit for the message to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Publish another message to a different user
    let message3 = NotificationMessage {
        title: "New Feature Available".to_string(),
        body: "Check out our new dashboard!".to_string(),
        timestamp: 1699900200,
    };

    pubsub
        .publish(
            MessageTopic::UserNotification,
            UserId("user456".to_string()),
            message3,
        )
        .await;

    // Keep the application running to receive messages
    println!("Waiting for messages (press Ctrl+C to exit)...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    println!("\nExample completed successfully!");

    Ok(())
}
