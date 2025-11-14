use fred::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum PubSubError {
    #[error("Unable to parse topic from str")]
    TopicParseError,

    #[error(transparent)]
    FredError(#[from] Error),

    #[error(transparent)]
    StdError(#[from] std::io::Error),

    #[error("Value Mismatched: {0}")]
    Mismatched(String),

    #[error("No receiver defined, set the receiver to start listening")]
    ReceiverError,
}
