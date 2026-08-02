//! Cache invalidation gRPC service.
//!
//! Handles `CacheRpc::Invalidate` requests from remote nodes
//! to invalidate local cache entries for objects that have been
//! modified or deleted.

use std::sync::Arc;

use oceanfs_cache::{MetadataCache, ObjectCache};
use oceanfs_core::{BucketId, ObjectKey};
use oceanfs_network::cache::{
    cache_rpc_server::CacheRpc, CacheInvalidateRequest, CacheInvalidateResponse,
};
use tonic::{Request, Response, Status};

/// gRPC service for cache invalidation.
pub struct CacheGrpcService {
    object_cache: Option<Arc<ObjectCache>>,
    metadata_cache: Option<Arc<MetadataCache>>,
}

impl CacheGrpcService {
    /// Creates a new cache gRPC service.
    pub fn new(
        object_cache: Option<Arc<ObjectCache>>,
        metadata_cache: Option<Arc<MetadataCache>>,
    ) -> Self {
        Self { object_cache, metadata_cache }
    }
}

#[tonic::async_trait]
impl CacheRpc for CacheGrpcService {
    async fn invalidate(
        &self,
        request: Request<CacheInvalidateRequest>,
    ) -> Result<Response<CacheInvalidateResponse>, Status> {
        let req = request.into_inner();

        let bucket_name = req.bucket_id.as_ref().map(|b| b.name.clone()).unwrap_or_default();
        let key_name = req.object_key.as_ref().map(|k| k.key.clone()).unwrap_or_default();

        let bucket = BucketId::new(&bucket_name);
        let key = ObjectKey::new(&key_name);

        // Invalidate object cache.
        if let Some(ref cache) = self.object_cache {
            cache.invalidate(&bucket, &key);
        }

        // Invalidate metadata cache.
        if let Some(ref cache) = self.metadata_cache {
            cache.invalidate(&bucket, &key);
        }

        Ok(Response::new(CacheInvalidateResponse { acknowledged: true }))
    }
}
