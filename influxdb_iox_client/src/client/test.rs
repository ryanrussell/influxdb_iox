/// Re-export generated_types
use generated_types::{
    i_ox_testing_client::IOxTestingClient, ErrorType as ProtoErrorType, TestErrorRequest,
};

use crate::connection::Connection;
use crate::error::Error;

/// A client for testing purposes
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     test::{Client, ErrorType},
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // trigger a panic
/// client
///     .error(ErrorType::Panic)
///     .await
///     .expect("failed to trigger an error");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: IOxTestingClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: IOxTestingClient::new(channel),
        }
    }

    /// Trigger an error.
    pub async fn error(&mut self, error_type: ErrorType) -> Result<(), Error> {
        let error_type: ProtoErrorType = error_type.into();
        let request = TestErrorRequest {
            error_type: error_type.into(),
        };
        self.inner.test_error(request).await?;
        Ok(())
    }
}

/// Describes how IOx should error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    /// Panics.
    Panic,

    /// Segfaults.
    Segfault,
}

impl From<ErrorType> for ProtoErrorType {
    fn from(t: ErrorType) -> Self {
        match t {
            ErrorType::Panic => ProtoErrorType::Panic,
            ErrorType::Segfault => ProtoErrorType::Segfault,
        }
    }
}
