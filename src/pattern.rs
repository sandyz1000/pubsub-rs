use regex::Regex;
use std::fmt::Debug;
use std::hash::Hash;

/// Pattern matcher for topic subscriptions
#[derive(Debug, Clone)]
pub enum TopicPattern<T>
where
    T: Debug + Clone,
{
    /// Exact topic match
    Exact(T),
    /// Wildcard pattern (supports * and ?)
    Wildcard(String),
    /// Regex pattern
    Regex(String, Regex),
    /// Match all topics
    All,
}

impl<T> TopicPattern<T>
where
    T: Debug + Clone + std::fmt::Display,
{
    /// Create an exact match pattern
    pub fn exact(topic: T) -> Self {
        TopicPattern::Exact(topic)
    }

    /// Create a wildcard pattern
    /// * matches any sequence of characters
    /// ? matches any single character
    pub fn wildcard(pattern: impl Into<String>) -> Self {
        TopicPattern::Wildcard(pattern.into())
    }

    /// Create a regex pattern
    pub fn regex(pattern: impl Into<String>) -> Result<Self, regex::Error> {
        let pattern_str = pattern.into();
        let regex = Regex::new(&pattern_str)?;
        Ok(TopicPattern::Regex(pattern_str, regex))
    }

    /// Create a match-all pattern
    pub fn all() -> Self {
        TopicPattern::All
    }

    /// Check if a topic matches this pattern
    pub fn matches(&self, topic: &T) -> bool {
        match self {
            TopicPattern::Exact(pattern_topic) => {
                format!("{:?}", pattern_topic) == format!("{:?}", topic)
            }
            TopicPattern::Wildcard(pattern) => Self::wildcard_match(pattern, &format!("{}", topic)),
            TopicPattern::Regex(_, regex) => regex.is_match(&format!("{}", topic)),
            TopicPattern::All => true,
        }
    }

    /// Wildcard matching implementation
    fn wildcard_match(pattern: &str, text: &str) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut text_chars = text.chars().peekable();

        while let Some(&p) = pattern_chars.peek() {
            match p {
                '*' => {
                    pattern_chars.next();
                    // If * is the last character, match everything
                    if pattern_chars.peek().is_none() {
                        return true;
                    }

                    // Try to match the rest of the pattern with remaining text
                    while text_chars.peek().is_some() {
                        if Self::wildcard_match(
                            &pattern_chars.clone().collect::<String>(),
                            &text_chars.clone().collect::<String>(),
                        ) {
                            return true;
                        }
                        text_chars.next();
                    }
                    return false;
                }
                '?' => {
                    pattern_chars.next();
                    if text_chars.next().is_none() {
                        return false;
                    }
                }
                _ => {
                    pattern_chars.next();
                    if text_chars.next() != Some(p) {
                        return false;
                    }
                }
            }
        }

        text_chars.peek().is_none()
    }
}

impl<T> PartialEq for TopicPattern<T>
where
    T: Debug + Clone + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TopicPattern::Exact(a), TopicPattern::Exact(b)) => a == b,
            (TopicPattern::Wildcard(a), TopicPattern::Wildcard(b)) => a == b,
            (TopicPattern::Regex(a, _), TopicPattern::Regex(b, _)) => a == b,
            (TopicPattern::All, TopicPattern::All) => true,
            _ => false,
        }
    }
}

impl<T> Eq for TopicPattern<T> where T: Debug + Clone + Eq {}

impl<T> Hash for TopicPattern<T>
where
    T: Debug + Clone + Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            TopicPattern::Exact(topic) => {
                "exact".hash(state);
                topic.hash(state);
            }
            TopicPattern::Wildcard(pattern) => {
                "wildcard".hash(state);
                pattern.hash(state);
            }
            TopicPattern::Regex(pattern, _) => {
                "regex".hash(state);
                pattern.hash(state);
            }
            TopicPattern::All => {
                "all".hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let pattern = TopicPattern::exact("test_topic");
        assert!(pattern.matches(&"test_topic"));
        assert!(!pattern.matches(&"other_topic"));
    }

    #[test]
    fn test_wildcard_match() {
        assert!(TopicPattern::<String>::wildcard_match(
            "user.*",
            "user.created"
        ));
        assert!(TopicPattern::<String>::wildcard_match(
            "user.*",
            "user.deleted"
        ));
        assert!(!TopicPattern::<String>::wildcard_match(
            "user.*",
            "admin.created"
        ));

        assert!(TopicPattern::<String>::wildcard_match(
            "*.event",
            "user.event"
        ));
        assert!(!TopicPattern::<String>::wildcard_match(
            "*.event",
            "user.action"
        ));
    }

    #[test]
    fn test_regex_match() {
        let pattern = TopicPattern::<String>::regex(r"^user\.(created|deleted)$").unwrap();
        assert!(pattern.matches(&"user.created".to_string()));
        assert!(pattern.matches(&"user.deleted".to_string()));
        assert!(!pattern.matches(&"user.updated".to_string()));
    }

    #[test]
    fn test_all_match() {
        let pattern = TopicPattern::<String>::all();
        assert!(pattern.matches(&"anything".to_string()));
        assert!(pattern.matches(&"everything".to_string()));
    }
}
