//! Error types for Aster database operations

/// Result type alias for Aster operations
pub type Result<T> = std::result::Result<T, AsterError>;

/// Comprehensive error type for all Aster database operations
#[derive(Debug, thiserror::Error)]
pub enum AsterError {
    /// I/O related errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// JSON parsing errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Storage engine errors
    #[error("Storage error: {message}")]
    Storage { message: String },

    /// Transaction errors
    #[error("Transaction error: {message}")]
    Transaction { message: String },

    /// Vertex not found
    #[error("Vertex not found: {id}")]
    VertexNotFound { id: String },

    /// Edge not found
    #[error("Edge not found: {id}")]
    EdgeNotFound { id: String },

    /// Invalid operation
    #[error("Invalid operation: {message}")]
    InvalidOperation { message: String },

    /// Concurrent access conflict
    #[error("Conflict detected: {message}")]
    Conflict { message: String },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Internal consistency errors
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Timeout errors
    #[error("Timeout: {message}")]
    Timeout { message: String },

    /// Recovery errors
    #[error("Recovery error: {message}")]
    Recovery { message: String },

    /// Corruption errors
    #[error("Corruption detected: {message}")]
    Corruption { message: String },
}

impl AsterError {
    /// Create a storage error
    pub fn storage<S: Into<String>>(message: S) -> Self {
        Self::Storage {
            message: message.into(),
        }
    }

    /// Create a transaction error
    pub fn transaction<S: Into<String>>(message: S) -> Self {
        Self::Transaction {
            message: message.into(),
        }
    }

    /// Create an invalid operation error
    pub fn invalid_operation<S: Into<String>>(message: S) -> Self {
        Self::InvalidOperation {
            message: message.into(),
        }
    }

    /// Create a conflict error
    pub fn conflict<S: Into<String>>(message: S) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// Create a configuration error
    pub fn configuration<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create an internal error
    pub fn internal<S: Into<String>>(message: S) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout<S: Into<String>>(message: S) -> Self {
        Self::Timeout {
            message: message.into(),
        }
    }

    /// Create a recovery error
    pub fn recovery<S: Into<String>>(message: S) -> Self {
        Self::Recovery {
            message: message.into(),
        }
    }

    /// Create a corruption error
    pub fn corruption<S: Into<String>>(message: S) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }
}

/// Trait for converting errors into AsterError
pub trait IntoAsterError<T> {
    fn into_aster_error(self) -> Result<T>;
}

impl<T, E: Into<AsterError>> IntoAsterError<T> for std::result::Result<T, E> {
    fn into_aster_error(self) -> Result<T> {
        self.map_err(|e| e.into())
    }
}
