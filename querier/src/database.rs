//! Database for the querier that contains all namespaces.

use crate::{
    cache::CatalogCache, chunk::ChunkAdapter, ingester::IngesterConnection,
    namespace::QuerierNamespace, query_log::QueryLog,
};
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{KafkaPartition, Namespace};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use parquet_file::storage::ParquetStorage;
use service_common::QueryDatabaseProvider;
use sharder::JumpHash;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, sync::Arc};
use tracker::{
    AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit, InstrumentedAsyncSemaphore,
};

/// The number of entries to store in the circular query buffer log.
///
/// That buffer is shared between all namespaces, and filtered on query
const QUERY_LOG_SIZE: usize = 10_000;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Catalog error: {source}"))]
    Catalog {
        source: iox_catalog::interface::Error,
    },

    #[snafu(display("Sharder error: {source}"))]
    Sharder { source: sharder::Error },
}

/// Database for the querier.
///
/// Contains all namespaces.
#[derive(Debug)]
pub struct QuerierDatabase {
    /// Backoff config for IO operations.
    backoff_config: BackoffConfig,

    /// Catalog cache.
    catalog_cache: Arc<CatalogCache>,

    /// Adapter to create chunks.
    chunk_adapter: Arc<ChunkAdapter>,

    /// Metric registry
    #[allow(dead_code)]
    metric_registry: Arc<metric::Registry>,

    /// Executor for queries.
    exec: Arc<Executor>,

    /// Connection to ingester(s)
    ingester_connection: Arc<dyn IngesterConnection>,

    /// Query log.
    query_log: Arc<QueryLog>,

    /// Semaphore that limits the number of namespaces in used at the time by the query subsystem.
    ///
    /// This should be a 1-to-1 relation to the number of active queries.
    ///
    /// If the same database is requested twice for different queries, it is counted twice.
    query_execution_semaphore: Arc<InstrumentedAsyncSemaphore>,

    /// Sharder to determine which ingesters to query for a particular table and namespace.
    sharder: Arc<JumpHash<Arc<KafkaPartition>>>,
}

#[async_trait]
impl QueryDatabaseProvider for QuerierDatabase {
    type Db = QuerierNamespace;

    async fn db(&self, name: &str) -> Option<Arc<Self::Db>> {
        self.namespace(name).await
    }

    async fn acquire_semaphore(&self) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_execution_semaphore)
            .acquire_owned()
            .await
            .expect("Semaphore should not be closed by anyone")
    }
}

impl QuerierDatabase {
    /// The maximum value for `max_concurrent_queries` that is allowed.
    ///
    /// This limit exists because [`tokio::sync::Semaphore`] has an internal limit and semaphore
    /// creation beyond that will panic. The tokio limit is not exposed though so we pick a
    /// reasonable but smaller number.
    pub const MAX_CONCURRENT_QUERIES_MAX: usize = u16::MAX as usize;

    /// Create new database.
    pub async fn new(
        catalog_cache: Arc<CatalogCache>,
        metric_registry: Arc<metric::Registry>,
        store: ParquetStorage,
        exec: Arc<Executor>,
        ingester_connection: Arc<dyn IngesterConnection>,
        max_concurrent_queries: usize,
    ) -> Result<Self, Error> {
        assert!(
            max_concurrent_queries <= Self::MAX_CONCURRENT_QUERIES_MAX,
            "`max_concurrent_queries` ({}) > `max_concurrent_queries_MAX` ({})",
            max_concurrent_queries,
            Self::MAX_CONCURRENT_QUERIES_MAX,
        );

        let backoff_config = BackoffConfig::default();

        let chunk_adapter = Arc::new(ChunkAdapter::new(
            Arc::clone(&catalog_cache),
            store,
            Arc::clone(&metric_registry),
            catalog_cache.time_provider(),
        ));
        let query_log = Arc::new(QueryLog::new(QUERY_LOG_SIZE, catalog_cache.time_provider()));
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metric_registry,
            &[("semaphore", "query_execution")],
        ));
        let query_execution_semaphore =
            Arc::new(semaphore_metrics.new_semaphore(max_concurrent_queries));

        let sharder = Arc::new(
            create_sharder(catalog_cache.catalog().as_ref(), backoff_config.clone()).await?,
        );

        Ok(Self {
            backoff_config,
            catalog_cache,
            chunk_adapter,
            metric_registry,
            exec,
            ingester_connection,
            query_log,
            query_execution_semaphore,
            sharder,
        })
    }

    /// Get namespace if it exists.
    ///
    /// This will await the internal namespace semaphore. Existence of namespaces is checked AFTER
    /// a semaphore permit was acquired since this lowers the chance that we obtain stale data.
    pub async fn namespace(&self, name: &str) -> Option<Arc<QuerierNamespace>> {
        let name = Arc::from(name.to_owned());
        let schema = self
            .catalog_cache
            .namespace()
            .schema(Arc::clone(&name))
            .await?;
        Some(Arc::new(QuerierNamespace::new(
            Arc::clone(&self.chunk_adapter),
            schema,
            name,
            Arc::clone(&self.exec),
            Arc::clone(&self.ingester_connection),
            Arc::clone(&self.query_log),
            Arc::clone(&self.sharder),
        )))
    }

    /// Return all namespaces this querier knows about
    pub async fn namespaces(&self) -> Vec<Namespace> {
        let catalog = &self.catalog_cache.catalog();
        Backoff::new(&self.backoff_config)
            .retry_all_errors("listing namespaces", || async {
                catalog.repositories().await.namespaces().list().await
            })
            .await
            .expect("retry forever")
    }

    /// Return connection to ingester(s) to get and aggregate information from them
    pub fn ingester_connection(&self) -> Arc<dyn IngesterConnection> {
        Arc::clone(&self.ingester_connection)
    }

    /// Executor
    pub(crate) fn exec(&self) -> &Executor {
        &self.exec
    }
}

pub async fn create_sharder(
    catalog: &dyn Catalog,
    backoff_config: BackoffConfig,
) -> Result<JumpHash<Arc<KafkaPartition>>, Error> {
    let sequencers = Backoff::new(&backoff_config)
        .retry_all_errors("get sequencers", || async {
            catalog.repositories().await.sequencers().list().await
        })
        .await
        .expect("retry forever");

    // Construct the (ordered) set of sequencers.
    //
    // The sort order must be deterministic in order for all nodes to shard to
    // the same sequencers, therefore we type assert the returned set is of the
    // ordered variety.
    let shards: BTreeSet<_> = sequencers
        //          ^ don't change this to an unordered set
        .into_iter()
        .map(|sequencer| sequencer.kafka_partition)
        .collect();

    JumpHash::new(shards.into_iter().map(Arc::new)).context(SharderSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_ingester_connection_for_testing;
    use iox_tests::util::TestCatalog;
    use test_helpers::assert_error;

    #[tokio::test]
    #[should_panic(
        expected = "`max_concurrent_queries` (65536) > `max_concurrent_queries_MAX` (65535)"
    )]
    async fn test_semaphore_limit_is_checked() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX.saturating_add(1),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn sequencers_in_catalog_are_required_for_startup() {
        let catalog = TestCatalog::new();

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));

        assert_error!(
            QuerierDatabase::new(
                catalog_cache,
                catalog.metric_registry(),
                ParquetStorage::new(catalog.object_store()),
                catalog.exec(),
                create_ingester_connection_for_testing(),
                QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
            )
            .await,
            Error::Sharder {
                source: sharder::Error::NoShards
            },
        );
    }

    #[tokio::test]
    async fn test_namespace() {
        let catalog = TestCatalog::new();
        // QuerierDatabase::new returns an error if there are no sequencers in the catalog
        catalog.create_sequencer(0).await;

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
        )
        .await
        .unwrap();

        catalog.create_namespace("ns1").await;

        assert!(db.namespace("ns1").await.is_some());
        assert!(db.namespace("ns2").await.is_none());
    }

    #[tokio::test]
    async fn test_namespaces() {
        let catalog = TestCatalog::new();
        // QuerierDatabase::new returns an error if there are no sequencers in the catalog
        catalog.create_sequencer(0).await;

        let catalog_cache = Arc::new(CatalogCache::new(
            catalog.catalog(),
            catalog.time_provider(),
            catalog.metric_registry(),
            usize::MAX,
        ));
        let db = QuerierDatabase::new(
            catalog_cache,
            catalog.metric_registry(),
            ParquetStorage::new(catalog.object_store()),
            catalog.exec(),
            create_ingester_connection_for_testing(),
            QuerierDatabase::MAX_CONCURRENT_QUERIES_MAX,
        )
        .await
        .unwrap();

        catalog.create_namespace("ns1").await;
        catalog.create_namespace("ns2").await;

        let mut namespaces = db.namespaces().await;
        namespaces.sort_by_key(|ns| ns.name.clone());
        assert_eq!(namespaces.len(), 2);
        assert_eq!(namespaces[0].name, "ns1");
        assert_eq!(namespaces[1].name, "ns2");
    }
}
