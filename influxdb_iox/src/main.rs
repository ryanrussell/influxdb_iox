//! Entrypoint of InfluxDB IOx binary
#![recursion_limit = "512"] // required for print_cpu
#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use crate::commands::{
    run::all_in_one,
    tracing::{init_logs_and_tracing, init_simple_logs, TroggingGuard},
};
use crash_handler::{CrashEvent, CrashHandler};
use dotenv::dotenv;
use influxdb_iox_client::connection::Builder;
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::warn;
use once_cell::sync::{Lazy, OnceCell};
use parking_lot::Mutex;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    str::FromStr,
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::runtime::Runtime;

mod commands {
    pub mod catalog;
    pub mod debug;
    pub mod query;
    pub mod query_ingester;
    pub mod remote;
    pub mod run;
    pub mod sql;
    pub mod storage;
    pub mod tracing;
    pub mod write;
}

enum ReturnCode {
    Failure = 1,
}

static VERSION_STRING: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        env!(
            "GIT_HASH",
            "Can not find find GIT HASH in build environment"
        )
    )
});

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
compile_error!("heappy and jemalloc_replacing_malloc features are mutually exclusive");

#[derive(Debug, clap::Parser)]
#[clap(
    name = "influxdb_iox",
    version = &VERSION_STRING[..],
    about = "InfluxDB IOx server and command line tools",
    long_about = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server in all-in-one "run" mode
    influxdb_iox

    # Display all available modes, including "run"
    influxdb_iox --help

    # Run the InfluxDB IOx server in all-in-one mode with extra verbose logging
    influxdb_iox -v

    # Run InfluxDB IOx with full debug logging specified with LOG_FILTER
    LOG_FILTER=debug influxdb_iox

    # Display all "run" mode settings
    influxdb_iox run --help

    # Run the interactive SQL prompt
    influxdb_iox sql

Command are generally structured in the form:
    <type of object> <action> <arguments>

For example, a command such as the following shows all actions
    available for database chunks, including get and list.

    influxdb_iox database chunk --help
"#
)]
struct Config {
    /// gRPC address of IOx server to connect to
    #[clap(
        short,
        long,
        global = true,
        env = "IOX_ADDR",
        default_value = "http://127.0.0.1:8082"
    )]
    host: String,

    /// Additional headers to add to CLI requests
    ///
    /// Values should be key value pairs separated by ':'
    #[clap(long, global = true)]
    header: Vec<KeyValue<http::header::HeaderName, http::HeaderValue>>,

    /// Configure the request timeout for CLI requests
    #[clap(long, global = true, default_value = "30s", parse(try_from_str = humantime::parse_duration))]
    rpc_timeout: Duration,

    /// Automatically generate an uber-trace-id header for CLI requests
    ///
    /// The generated trace ID will be emitted at the beginning of the response.
    #[clap(long, global = true)]
    gen_trace_id: bool,

    /// Set the maximum number of threads to use. Defaults to the number of
    /// cores on the system
    #[clap(long)]
    num_threads: Option<usize>,

    /// Supports having all-in-one be the default command.
    #[clap(flatten)]
    all_in_one_config: all_in_one::Config,

    #[clap(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Run the InfluxDB IOx server
    // Clippy recommended boxing this variant because it's much larger than the others
    Run(Box<commands::run::Config>),

    /// Commands to run against remote IOx APIs
    Remote(commands::remote::Config),

    /// Start IOx interactive SQL REPL loop
    Sql(commands::sql::Config),

    /// Various commands for catalog manipulation
    Catalog(commands::catalog::Config),

    /// Interrogate internal database data
    Debug(commands::debug::Config),

    /// Initiate a read request to the gRPC storage service.
    Storage(commands::storage::Config),

    /// Write data into the specified database
    Write(commands::write::Config),

    /// Query the data with SQL
    Query(commands::query::Config),

    /// Query the ingester only
    QueryIngester(commands::query_ingester::Config),
}

fn main() -> Result<(), std::io::Error> {
    install_crash_handler(); // attempt to render a useful stacktrace to stderr

    // load all environment variables from .env before doing anything
    load_dotenv();

    let config: Config = clap::Parser::parse();

    let tokio_runtime = get_runtime(config.num_threads)?;
    tokio_runtime.block_on(async move {
        let host = config.host;
        let headers = config.header;
        let log_verbose_count = config.all_in_one_config.logging_config.log_verbose_count;
        let rpc_timeout = config.rpc_timeout;

        let connection = || async move {
            let mut builder = headers.into_iter().fold(Builder::default(), |builder, kv| {
                builder.header(kv.key, kv.value)
            });

            builder = builder.timeout(rpc_timeout);

            if config.gen_trace_id {
                let key = http::header::HeaderName::from_str(
                    trace_exporters::DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME,
                )
                .unwrap();
                let trace_id = gen_trace_id();
                let value = http::header::HeaderValue::from_str(trace_id.as_str()).unwrap();
                builder = builder.header(key, value);

                // Emit trace id information
                println!("Trace ID set to {}", trace_id);
            }

            match builder.build(&host).await {
                Ok(connection) => connection,
                Err(e) => {
                    eprintln!("Error connecting to {}: {}", host, e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        };

        fn handle_init_logs(r: Result<TroggingGuard, trogging::Error>) -> TroggingGuard {
            match r {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Initializing logs failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _);
                }
            }
        }

        match config.command {
            None => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = all_in_one::command(config.all_in_one_config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Remote(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::remote::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Run(config)) => {
                let _tracing_guard =
                    handle_init_logs(init_logs_and_tracing(log_verbose_count, &config));
                if let Err(e) = commands::run::command(*config).await {
                    eprintln!("Server command failed: {}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Sql(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::sql::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Storage(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::storage::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Catalog(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::catalog::command(config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Debug(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                if let Err(e) = commands::debug::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Write(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::write::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::Query(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::query::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
            Some(Command::QueryIngester(config)) => {
                let _tracing_guard = handle_init_logs(init_simple_logs(log_verbose_count));
                let connection = connection().await;
                if let Err(e) = commands::query_ingester::command(connection, config).await {
                    eprintln!("{}", e);
                    std::process::exit(ReturnCode::Failure as _)
                }
            }
        }
    });

    Ok(())
}

// Generates a compatible header values for a jaeger trace context header.
fn gen_trace_id() -> String {
    let now = SystemProvider::new().now();
    let mut hasher = DefaultHasher::new();
    now.timestamp_nanos().hash(&mut hasher);

    format!("{:x}:1112223334445:0:1", hasher.finish())
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<usize>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );

            match num_threads {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(num_threads)
                    .build(),
            }
        }
    }
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}

static CRASH_HANDLER: OnceCell<CrashHandler> = OnceCell::new();

fn install_crash_handler() {
    // ignore error because it is OK to install the handler twice
    if let Ok(handler) = CrashHandler::attach(Box::new(CrashEventImpl::new())) {
        CRASH_HANDLER.set(handler).ok();
    } else {
        assert!(
            CRASH_HANDLER.get().is_some(),
            "Cannot install crash handler"
        );
    }
}

enum CrashPrinterCmd {
    Exit,
    Print {
        frame: backtrace::Frame,
        frame_count: usize,
    },
}

struct CrashEventImpl {
    /// Join handle for printer thread.
    handle: Option<JoinHandle<()>>,

    /// Channel to send commands to the printer thread as well as and ID counter.
    ///
    /// They are behind the same mutex to avoid reordering between "picking an ID" and "sending the message".
    ///
    /// The channel is bound and sending data should not allocate.
    tx_and_counter: Mutex<(SyncSender<CrashPrinterCmd>, u64)>,

    /// Counter of which message is done.
    done: Arc<AtomicU64>,

    /// Mutex that avoids that we try to handle and print out two signals at the same time.
    currently_handles_signal: Mutex<()>,
}

impl CrashEventImpl {
    fn new() -> Self {
        let (tx, rx) = sync_channel(1);
        let done = Arc::new(AtomicU64::new(0));
        let done_captured = Arc::clone(&done);
        let handle = std::thread::spawn(move || {
            // While we avoid allocations in this thread as good as possible, `backtrace` may always allocate when
            // resolving symbols, so we cannot do much about i.t
            loop {
                let cmd = match rx.recv() {
                    Ok(cmd) => cmd,
                    Err(_) => {
                        // sender is gone => exit
                        return;
                    }
                };

                match cmd {
                    CrashPrinterCmd::Exit => {
                        return;
                    }
                    CrashPrinterCmd::Print { frame, frame_count } => {
                        let ip = frame.ip();
                        let symbol_address = frame.symbol_address();

                        eprintln!("  {} ({:p}):  ", frame_count, ip);

                        // Resolve this instruction pointer to a symbol name(s) (there can be multiple!)
                        // must use the unsync version because the sync version deadlocks
                        unsafe {
                            backtrace::resolve_frame_unsynchronized(&frame, |symbol| {
                                eprint!("      ");
                                if let Some(name) = symbol.name() {
                                    eprint!("{}", name)
                                } else {
                                    eprint!("<unresolved>");
                                }

                                eprintln!("({:p})", symbol_address);

                                eprint!("        at ");
                                if let Some(filename) = symbol.filename() {
                                    eprint!("{}", filename.to_string_lossy());
                                } else {
                                    eprint!("<unknown>");
                                }
                                eprint!(":");
                                if let Some(lineno) = symbol.lineno() {
                                    eprint!("{}", lineno);
                                } else {
                                    eprint!("<unknown>");
                                }
                                eprint!(":");
                                if let Some(colno) = symbol.colno() {
                                    eprint!("{}", colno);
                                } else {
                                    eprint!("<unknown>");
                                }
                                eprintln!();
                            })
                        };

                        done_captured.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        let tx_and_counter = Mutex::new((tx, 0));

        Self {
            handle: Some(handle),
            tx_and_counter,
            done,
            currently_handles_signal: Mutex::new(()),
        }
    }
}

impl Drop for CrashEventImpl {
    fn drop(&mut self) {
        let guard = self.tx_and_counter.lock();
        guard.0.send(CrashPrinterCmd::Exit).ok();

        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

unsafe impl CrashEvent for CrashEventImpl {
    fn on_crash(&self, context: &crash_handler::CrashContext) -> crash_handler::CrashEventResult {
        let _guard = self.currently_handles_signal.lock();

        // We MUST do as little as possible in this context. Esp. we must avoid allocations. For that we only try to use
        // stack-allocated data structures and let the actual stack frame printing (which allocates, see
        // https://github.com/rust-lang/backtrace-rs/pull/265#discussion_r352654555 ) be done by an ordinary thread.
        eprintln!("!!! CRASH REPORT START !!!",);

        eprintln!("Signal: {}", context.siginfo.ssi_signo,);

        if let Some(name) = std::thread::current().name() {
            eprintln!("Thread: {}", name);
        } else {
            eprintln!("Thread: <unknown>");
        }

        eprintln!("Stack trace:\n",);
        let mut frame_count = 0;
        backtrace::trace(|frame| {
            let id = {
                let mut guard = self.tx_and_counter.lock();
                let id = guard.1;
                guard.1 += 1;
                match guard.0.send(CrashPrinterCmd::Print {
                    frame: frame.clone(),
                    frame_count,
                }) {
                    Ok(()) => id,
                    Err(_) => {
                        eprintln!("  <cannot print>");
                        return false;
                    }
                }
            };

            loop {
                if self.done.load(Ordering::SeqCst) >= id {
                    break;
                }
            }

            frame_count += 1;
            true // keep going to the next frame
        });

        eprintln!("!!! CRASH REPORT END !!!",);

        crash_handler::CrashEventResult::Handled(false)
    }
}

/// A ':' separated key value pair
#[derive(Debug, Clone)]
struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> std::str::FromStr for KeyValue<K, V>
where
    K: FromStr,
    V: FromStr,
    K::Err: std::fmt::Display,
    V::Err: std::fmt::Display,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use itertools::Itertools;
        match s.splitn(2, ':').collect_tuple() {
            Some((key, value)) => {
                let key = K::from_str(key).map_err(|e| e.to_string())?;
                let value = V::from_str(value).map_err(|e| e.to_string())?;
                Ok(Self { key, value })
            }
            None => Err(format!(
                "Invalid key value pair - expected 'KEY:VALUE' got '{}'",
                s
            )),
        }
    }
}
