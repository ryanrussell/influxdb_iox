//! gRPC service implementations for `router`.

use data_types::{QueryPoolId, TopicId};
use generated_types::influxdata::iox::{catalog::v1::*, namespace::v1::*, object_store::v1::*};
use iox_catalog::interface::Catalog;
use object_store::DynObjectStore;
use service_grpc_catalog::CatalogService;
use service_grpc_namespace::NamespaceService;
use service_grpc_object_store::ObjectStoreService;
use service_grpc_schema::SchemaService;
use std::sync::Arc;

/// This type manages all gRPC services exposed by a `router` using the RPC write path.
#[derive(Debug)]
pub struct RpcWriteGrpcDelegate {
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,

    // Temporary values during kafka -> kafkaless transition.
    topic_id: TopicId,
    query_id: QueryPoolId,
}

impl RpcWriteGrpcDelegate {
    /// Create a new gRPC handler
    pub fn new(
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        topic_id: TopicId,
        query_id: QueryPoolId,
    ) -> Self {
        Self {
            catalog,
            object_store,
            topic_id,
            query_id,
        }
    }

    /// Acquire a [`SchemaService`] gRPC service implementation.
    ///
    /// [`SchemaService`]: generated_types::influxdata::iox::schema::v1::schema_service_server::SchemaService.
    pub fn schema_service(&self) -> SchemaService {
        SchemaService::new(Arc::clone(&self.catalog))
    }

    /// Acquire a [`CatalogService`] gRPC service implementation.
    ///
    /// [`CatalogService`]: generated_types::influxdata::iox::catalog::v1::catalog_service_server::CatalogService.
    pub fn catalog_service(&self) -> impl catalog_service_server::CatalogService {
        CatalogService::new(Arc::clone(&self.catalog))
    }

    /// Acquire a [`ObjectStoreService`] gRPC service implementation.
    ///
    /// [`ObjectStoreService`]: generated_types::influxdata::iox::object_store::v1::object_store_service_server::ObjectStoreService.
    pub fn object_store_service(&self) -> impl object_store_service_server::ObjectStoreService {
        ObjectStoreService::new(Arc::clone(&self.catalog), Arc::clone(&self.object_store))
    }

    /// Acquire a [`NamespaceService`] gRPC service implementation.
    ///
    /// [`NamespaceService`]: generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService.
    pub fn namespace_service(&self) -> impl namespace_service_server::NamespaceService {
        NamespaceService::new(
            Arc::clone(&self.catalog),
            Some(self.topic_id),
            Some(self.query_id),
        )
    }
}
