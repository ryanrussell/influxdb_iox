use snafu::{ResultExt, Snafu};
use trogging::cli::LoggingConfig;

pub(crate) mod all_in_one;
mod compactor2;
mod garbage_collector;
mod ingester2;
mod main;
mod querier;
mod router2;
mod test;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Error in compactor2 subcommand: {}", source))]
    Compactor2Error { source: compactor2::Error },

    #[snafu(display("Error in garbage collector subcommand: {}", source))]
    GarbageCollectorError { source: garbage_collector::Error },

    #[snafu(display("Error in querier subcommand: {}", source))]
    QuerierError { source: querier::Error },

    #[snafu(display("Error in router2 subcommand: {}", source))]
    Router2Error { source: router2::Error },

    #[snafu(display("Error in ingester2 subcommand: {}", source))]
    Ingester2Error { source: ingester2::Error },

    #[snafu(display("Error in all in one subcommand: {}", source))]
    AllInOneError { source: all_in_one::Error },

    #[snafu(display("Error in test subcommand: {}", source))]
    TestError { source: test::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Supports having all-in-one be the default command.
    #[clap(flatten)]
    all_in_one_config: all_in_one::Config,

    #[clap(subcommand)]
    command: Option<Command>,
}

impl Config {
    pub fn logging_config(&self) -> &LoggingConfig {
        match &self.command {
            None => &self.all_in_one_config.logging_config,
            Some(Command::Compactor2(config)) => config.run_config.logging_config(),
            Some(Command::GarbageCollector(config)) => config.run_config.logging_config(),
            Some(Command::Querier(config)) => config.run_config.logging_config(),
            Some(Command::Router2(config)) => config.run_config.logging_config(),
            Some(Command::Ingester2(config)) => config.run_config.logging_config(),
            Some(Command::AllInOne(config)) => &config.logging_config,
            Some(Command::Test(config)) => config.run_config.logging_config(),
        }
    }
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Run the server in compactor2 mode
    Compactor2(compactor2::Config),

    /// Run the server in querier mode
    Querier(querier::Config),

    /// Run the server in router2 mode
    Router2(router2::Config),

    /// Run the server in ingester2 mode
    Ingester2(ingester2::Config),

    /// Run the server in "all in one" mode (Default)
    AllInOne(all_in_one::Config),

    /// Run the server in test mode
    Test(test::Config),

    /// Run the server in garbage collecter mode
    GarbageCollector(garbage_collector::Config),
}

pub async fn command(config: Config) -> Result<()> {
    match config.command {
        None => all_in_one::command(config.all_in_one_config)
            .await
            .context(AllInOneSnafu),
        Some(Command::Compactor2(config)) => {
            compactor2::command(config).await.context(Compactor2Snafu)
        }
        Some(Command::GarbageCollector(config)) => garbage_collector::command(config)
            .await
            .context(GarbageCollectorSnafu),
        Some(Command::Querier(config)) => querier::command(config).await.context(QuerierSnafu),
        Some(Command::Router2(config)) => router2::command(config).await.context(Router2Snafu),
        Some(Command::Ingester2(config)) => {
            ingester2::command(config).await.context(Ingester2Snafu)
        }
        Some(Command::AllInOne(config)) => all_in_one::command(config).await.context(AllInOneSnafu),
        Some(Command::Test(config)) => test::command(config).await.context(TestSnafu),
    }
}
