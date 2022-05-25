use generated_types::i_ox_testing_server::{IOxTesting, IOxTestingServer};
use generated_types::{ErrorType, TestErrorRequest, TestErrorResponse};
use observability_deps::tracing::warn;

/// Concrete implementation of the gRPC IOx testing service API
struct IOxTestingService {}

#[tonic::async_trait]
impl IOxTesting for IOxTestingService {
    async fn test_error(
        &self,
        req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, tonic::Status> {
        let req = req.into_inner();
        match req.error_type() {
            ErrorType::Unspecified | ErrorType::Panic => {
                warn!("Got a test_error request. About to panic");
                // Purposely do not use a static string (so that the panic
                // code has to deal with aribtrary payloads). See
                // https://github.com/influxdata/influxdb_iox/issues/1953
                panic!("This {}", "is a test panic");
            }
            ErrorType::Segfault => {
                warn!("Got a test_error request. About to segfault");

                segfault()
            }
        }
    }
}

// Only support segfault kills in debug builds.
//
// The internet suggests that the only way to detect debug/release builds is the `debug_assertions` cfg state, so we'll
// use that.
#[cfg(debug_assertions)]
fn segfault() -> ! {
    unsafe { std::ptr::null_mut::<i32>().write(42) };
    panic!("We should have segfaulted!");
}

#[cfg(not(debug_assertions))]
fn segfault() -> ! {
    panic!("Killing a process via segfault is NOT supported in release builds!");
}

pub fn make_server() -> IOxTestingServer<impl IOxTesting> {
    IOxTestingServer::new(IOxTestingService {})
}
