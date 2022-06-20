use chrono::{DateTime, Utc};
use iox_catalog::interface::ParquetFileRepo;
use object_store::ObjectMeta;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Could not create the catalog"))]
    CreatingCatalog {
        source: clap_blocks::catalog_dsn::Error,
    },

    #[snafu(display("Expected a file name"))]
    FileNameMissing,

    #[snafu(display(r#""{uuid}" is not a valid ID"#))]
    MalformedId { source: uuid::Error, uuid: String },

    #[snafu(display("The catalog could not be queried for {object_store_id}"))]
    GetFile {
        source: iox_catalog::interface::Error,
        object_store_id: uuid::Uuid,
    },

    #[snafu(display("The deleter task exited unexpectedly"))]
    DeleterExited {
        source: tokio::sync::mpsc::error::SendError<ObjectMeta>,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn perform(
    args: Arc<crate::Args>,
    mut items: mpsc::Receiver<ObjectMeta>,
    deleter: mpsc::Sender<ObjectMeta>,
) -> Result<()> {
    let catalog = args.catalog().await.context(CreatingCatalogSnafu)?;
    let cutoff = args.cutoff();

    let mut repositories = catalog.repositories().await;
    let parquet_files = repositories.parquet_files();

    while let Some(item) = items.recv().await {
        if should_delete(&item, cutoff, parquet_files).await? {
            deleter.send(item).await.context(DeleterExitedSnafu)?;
        }
    }

    Ok(())
}

async fn should_delete(
    item: &ObjectMeta,
    cutoff: DateTime<Utc>,
    parquet_files: &mut dyn ParquetFileRepo,
) -> Result<bool> {
    if cutoff < item.last_modified {
        // Not old enough; do not delete
        return Ok(false);
    }

    let file_name = item.location.parts().last().context(FileNameMissingSnafu)?;
    let file_name = file_name.to_string(); // TODO: Hmmmmmm; can we avoid allocation?

    if let Some(uuid) = file_name.strip_suffix(".parquet") {
        let object_store_id = uuid.parse().context(MalformedIdSnafu { uuid })?;
        let parquet_file = parquet_files
            .get_by_object_store_id(object_store_id)
            .await
            .context(GetFileSnafu { object_store_id })?;

        if parquet_file.is_some() {
            // We have a reference to this file; do not delete
            return Ok(false);
        }
    } else {
        return Ok(true)
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use iox_catalog::{interface::Catalog, mem::MemCatalog};
    use object_store::path::Path;

    async fn test_catalog() -> (Arc<dyn Catalog>, ParquetFile) {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repos = catalog.repositories().await;
        let kafka = repos.kafka_topics().create_or_get("foo").await.unwrap();
        let pool = repos.query_pools().create_or_get("foo").await.unwrap();
        let namespace = repos
            .namespaces()
            .create("namespace_parquet_file_test", "inf", kafka.id, pool.id)
            .await
            .unwrap();
        let table = repos
            .tables()
            .create_or_get("test_table", namespace.id)
            .await
            .unwrap();

        let parquet_file_params = ParquetFileParams {
            sequencer_id: sequencer.id,
            namespace_id: namespace.id,
            table_id: partition.table_id,
            partition_id: partition.id,
            object_store_id,
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(140),
            min_time: Timestamp::new(1),
            max_time: Timestamp::new(10),
            file_size_bytes: 1337,
            parquet_metadata: b"md1".to_vec(),
            row_count: 0,
            compaction_level: INITIAL_COMPACTION_LEVEL,
            created_at: Timestamp::new(1),
        };

        let parquet_file = repos.parquet_files().create(parquet_file_params).unwrap();

        (catalog, parquet_file)
    }


    #[tokio::test]
    async fn dont_delete_new_file_in_catalog() {
        let (catalog, file_in_catalog) = test_catalog().await;

        let cutoff = Utc.datetime_from_str("2022-01-01T00:00:00z", "%+").unwrap();
        let last_modified = Utc.datetime_from_str("2022-02-02T00:00:00z", "%+").unwrap();

        let item = ObjectMeta {
            location,
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn dont_delete_new_file_not_in_catalog() {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let mut repositories = catalog.repositories().await;
        let parquet_files = repositories.parquet_files();

        let cutoff = Utc.datetime_from_str("2022-01-01T00:00:00z", "%+").unwrap();
        let last_modified = Utc.datetime_from_str("2022-02-02T00:00:00z", "%+").unwrap();

        let item = ObjectMeta {
            location: Path::from_raw("irrelevant"),
            last_modified,
            size: 0,
        };

        assert!(!should_delete(&item, cutoff, parquet_files).await.unwrap());
    }

    #[tokio::test]
    async fn dont_delete_new_file_with_unparseable_path() {}

    #[tokio::test]
    async fn dont_delete_old_file_in_catalog() {}

    #[tokio::test]
    async fn delete_old_file_not_in_catalog() {}

    #[tokio::test]
    async fn delete_old_file_with_unparseable_path() {}
}
