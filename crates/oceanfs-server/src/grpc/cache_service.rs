//! Cache invalidation gRPC service.
//!
//! Handles `CacheRpc::Invalidate` requests from remote nodes
//! to invalidate local cache entries for objects that have been
//! modified or deleted.

use std::sync::Arc;

use oceanfs_cache::{
    cache::{cache_rpc_server::CacheRpc, CacheInvalidateRequest, CacheInvalidateResponse},
    MetadataCache, NegativeCache, ObjectCache,
};
use oceanfs_core::{BucketId, ObjectKey};
use tonic::{Request, Response, Status};

/// gRPC service for cache invalidation.
pub struct CacheGrpcService {
    object_cache: Option<Arc<ObjectCache>>,
    metadata_cache: Option<Arc<MetadataCache>>,
    negative_cache: Option<Arc<NegativeCache>>,
}

impl CacheGrpcService {
    /// Creates a new cache gRPC service.
    pub fn new(
        object_cache: Option<Arc<ObjectCache>>,
        metadata_cache: Option<Arc<MetadataCache>>,
        negative_cache: Option<Arc<NegativeCache>>,
    ) -> Self {
        Self { object_cache, metadata_cache, negative_cache }
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

        // Clear the L3 negative cache too: a node that answered 404 for a
        // key (and recorded it here) would otherwise keep serving "absent"
        // after a replica re-PUTs it — the cross-node PUT-after-DELETE
        // stale-404 divergence. The PUT path clears L3 locally; this
        // covers the replica-invalidation path.
        if let Some(ref cache) = self.negative_cache {
            cache.invalidate(&bucket, &key);
        }

        Ok(Response::new(CacheInvalidateResponse { acknowledged: true }))
    }
}

#[cfg(test)]
mod tests {
    use oceanfs_cache::NegativeCacheConfig;

    use super::*;

    /// Fix 3 regression: a remote invalidate (a replica re-PUT) must
    /// clear the L3 negative entry, or the node keeps serving a stale
    /// 404 for a key that now exists.
    #[tokio::test]
    async fn remote_invalidate_clears_negative_cache() {
        let negative = Arc::new(NegativeCache::new(NegativeCacheConfig {
            enabled: true,
            ..Default::default()
        }));
        let bucket = BucketId::new("b");
        let key = ObjectKey::new("k");

        // Simulate a prior 404 recorded on this node.
        negative.insert(&bucket, &key);
        assert!(negative.contains(&bucket, &key), "L3 must hold the absent key");

        let service = CacheGrpcService::new(None, None, Some(Arc::clone(&negative)));
        let proto_bucket: oceanfs_core::proto::common::BucketId = bucket.clone().into();
        let proto_key: oceanfs_core::proto::common::ObjectKey = key.clone().into();
        let req = CacheInvalidateRequest {
            bucket_id: Some(proto_bucket),
            object_key: Some(proto_key),
            invalidation_type: 0,
        };
        let resp = service.invalidate(Request::new(req)).await.unwrap();
        assert!(resp.into_inner().acknowledged);
        assert!(
            !negative.contains(&bucket, &key),
            "a remote invalidate must clear the L3 negative entry"
        );
    }
}
