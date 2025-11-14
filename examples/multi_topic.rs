use async_trait::async_trait;
use pubsub::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Event types
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum EventTopic {
    Order,
    Payment,
    Inventory,
}

impl std::str::FromStr for EventTopic {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Order" => Ok(EventTopic::Order),
            "Payment" => Ok(EventTopic::Payment),
            "Inventory" => Ok(EventTopic::Inventory),
            _ => Err(format!("Unknown topic: {}", s)),
        }
    }
}

type EntityId = String;

// Different event payloads
#[derive(Debug, Serialize, Deserialize)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    total: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PaymentEvent {
    payment_id: String,
    order_id: String,
    amount: f64,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct InventoryEvent {
    product_id: String,
    quantity: i32,
    action: String,
}

// Order receiver
#[derive(Debug)]
struct OrderReceiver;

#[async_trait]
impl PubSubReceiver<EventTopic> for OrderReceiver {
    type Key = EntityId;

    async fn notify(
        &self,
        _topic: EventTopic,
        key: Self::Key,
        payload: &[u8],
    ) -> Result<(), PubSubError> {
        let event: OrderEvent = serde_json::from_slice(payload)
            .map_err(|e| PubSubError::Mismatched(format!("Failed to deserialize: {}", e)))?;

        println!(
            "🛒 [ORDER] Key: {} | Order #{} from customer {} - Total: ${:.2}",
            key, event.order_id, event.customer_id, event.total
        );
        Ok(())
    }
}

// Payment receiver
#[derive(Debug)]
struct PaymentReceiver;

#[async_trait]
impl PubSubReceiver<EventTopic> for PaymentReceiver {
    type Key = EntityId;

    async fn notify(
        &self,
        _topic: EventTopic,
        key: Self::Key,
        payload: &[u8],
    ) -> Result<(), PubSubError> {
        let event: PaymentEvent = serde_json::from_slice(payload)
            .map_err(|e| PubSubError::Mismatched(format!("Failed to deserialize: {}", e)))?;

        println!(
            "💳 [PAYMENT] Key: {} | Payment #{} for order {} - ${:.2} [{}]",
            key, event.payment_id, event.order_id, event.amount, event.status
        );
        Ok(())
    }
}

// Inventory receiver
#[derive(Debug)]
struct InventoryReceiver;

#[async_trait]
impl PubSubReceiver<EventTopic> for InventoryReceiver {
    type Key = EntityId;

    async fn notify(
        &self,
        _topic: EventTopic,
        key: Self::Key,
        payload: &[u8],
    ) -> Result<(), PubSubError> {
        let event: InventoryEvent = serde_json::from_slice(payload)
            .map_err(|e| PubSubError::Mismatched(format!("Failed to deserialize: {}", e)))?;

        println!(
            "📦 [INVENTORY] Key: {} | Product {} - {} {} units",
            key, event.product_id, event.action, event.quantity
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏪 Multi-Topic E-Commerce Event System\n");

    let redis_url = "redis://127.0.0.1:6379/0".to_string();

    // Create instance with custom topic prefix
    let pubsub = PubSubRedis::<EventTopic, EntityId>::with_options(
        redis_url,
        Some("ecommerce".to_string()),
        None,
    )
    .await?;

    // Register different receivers for different topics
    pubsub.set_receiver(EventTopic::Order, Arc::new(OrderReceiver));
    pubsub.set_receiver(EventTopic::Payment, Arc::new(PaymentReceiver));
    pubsub.set_receiver(EventTopic::Inventory, Arc::new(InventoryReceiver));

    println!("✅ All receivers registered\n");

    // Start subscriber
    pubsub.clone().run_subscriber().await?;
    println!("👂 Subscriber listening for events...\n");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Simulate e-commerce events
    println!("📤 Publishing events...\n");

    // 1. Create order
    let order = OrderEvent {
        order_id: "ORD-001".to_string(),
        customer_id: "CUST-123".to_string(),
        total: 299.99,
    };
    pubsub
        .publish(EventTopic::Order, "ORD-001".to_string(), order)
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. Update inventory
    let inventory = InventoryEvent {
        product_id: "PROD-456".to_string(),
        quantity: 5,
        action: "reserved".to_string(),
    };
    pubsub
        .publish(EventTopic::Inventory, "PROD-456".to_string(), inventory)
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 3. Process payment
    let payment = PaymentEvent {
        payment_id: "PAY-789".to_string(),
        order_id: "ORD-001".to_string(),
        amount: 299.99,
        status: "completed".to_string(),
    };
    pubsub
        .publish(EventTopic::Payment, "PAY-789".to_string(), payment)
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Update inventory again
    let inventory2 = InventoryEvent {
        product_id: "PROD-456".to_string(),
        quantity: 5,
        action: "deducted".to_string(),
    };
    pubsub
        .publish(EventTopic::Inventory, "PROD-456".to_string(), inventory2)
        .await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    println!("\n✅ All events processed!");
    Ok(())
}
