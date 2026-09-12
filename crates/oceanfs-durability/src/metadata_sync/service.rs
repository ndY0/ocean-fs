//! Server-side metadata-sync handlers (ADR-0038 D3/D5; ae2 / S2).
//!
//! [`MetadataSyncService`] is the enabled-only bundle the healing gRPC
//! service holds: the journal (pull source), the durable watermarks
//! (bidirectional acks), and the metadata store (point fetch).

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use oceanfs_core::{BucketId, NodeId, ObjectKey};
use oceanfs_storage::{metadata::split_object_row_key, JournalRead, MetadataJournal};
use oceanfs_storage_api::MetadataStore;
use tonic::Status;

use crate::{
    healing_rpc::{
        metadata_row, DeletionRow, FetchJournalRequest, FetchJournalResponse,
        JournalEntry as WireJournalEntry, MetadataRow, ObjectRow,
    },
    metadata_sync::watermark::WatermarkStore,
};

/// Defensive server-side bounds for one `FetchJournal` read (a hostile or
/// buggy requester cannot force an unbounded read; the worker's own
/// budgets are much smaller).
const MAX_SERVER_ENTRIES: usize = 65_536;
const MAX_SERVER_BYTES: u64 = 16 * 1024 * 1024;

/// Enabled-only server-side metadata-sync state.
///
/// # Examples
///
/// ```ignore
/// let service = MetadataSyncService::new(journal, watermarks, metadata_store);
/// healing = healing.with_metadata_sync(service);
/// ```
pub struct MetadataSyncService {
    journal: Arc<MetadataJournal>,
    watermarks: Arc<WatermarkStore>,
    metadata_store: Arc<dyn MetadataStore>,
}

impl std::fmt::Debug for MetadataSyncService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataSyncService")
            .field("epoch", &self.journal.epoch())
            .finish_non_exhaustive()
    }
}

impl MetadataSyncService {
    /// Bundles the journal, watermarks, and metadata store for the
    /// enabled handlers.
    /// # Examples
    ///
    /// ```ignore
    /// let service = MetadataSyncService::new(journal, watermarks, store);
    /// ```
    pub fn new(
        journal: Arc<MetadataJournal>,
        watermarks: Arc<WatermarkStore>,
        metadata_store: Arc<dyn MetadataStore>,
    ) -> Arc<Self> {
        Arc::new(Self { journal, watermarks, metadata_store })
    }

    /// The journal this service serves.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(service.journal().epoch().as_bytes().len(), 16);
    /// ```
    pub fn journal(&self) -> &Arc<MetadataJournal> {
        &self.journal
    }

    /// The watermark store this service records acks into.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// service.watermarks().flush()?;
    /// ```
    pub fn watermarks(&self) -> &Arc<WatermarkStore> {
        &self.watermarks
    }

    /// Serves one bounded journal pull. Blocking (file I/O + fsync of a
    /// lazily flushed ack); callers run it on the blocking pool.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let response = service.fetch_journal(request)?;
    /// if response.entries.is_empty() && request.from_seq < response.oldest_seq {
    ///     // gap
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Status` when the journal read fails.
    pub fn fetch_journal(
        &self,
        request: FetchJournalRequest,
    ) -> Result<FetchJournalResponse, String> {
        let epoch = self.journal.epoch();
        let epoch_matches =
            request.epoch.len() == 16 && request.epoch.as_ref() == epoch.as_bytes().as_slice();
        // Opportunistic ack: the requester's `from_seq` is its
        // `consumed(this node)` — meaningful only when the requester's
        // view of OUR epoch matches the journal it actually consumed.
        // A foreign-epoch high watermark must never be recorded as an
        // ack (it would inflate the trim frontier).
        let requester = std::str::from_utf8(&request.requester_id)
            .ok()
            .filter(|id| !id.is_empty())
            .map(NodeId::new);
        if let Some(requester) = &requester {
            if request.from_seq > 0 && epoch_matches {
                self.watermarks.set_acked(requester, *epoch.as_bytes(), request.from_seq);
                if let Err(error) = self.watermarks.flush_peer(requester) {
                    tracing::warn!(%error, "metadata_sync: ack flush failed");
                }
            }
        }
        // The responder's `consumed(requester)` refers to the requester's
        // journal epoch we last saw; send it back so the requester can
        // validate the ack.
        let acknowledged_epoch = requester
            .as_ref()
            .and_then(|peer| self.watermarks.get(peer).epoch)
            .map(|epoch| Bytes::copy_from_slice(&epoch))
            .unwrap_or_default();

        let oldest_seq = self.journal.oldest_seq();
        let acknowledged_seq =
            requester.as_ref().map(|peer| self.watermarks.get(peer).consumed).unwrap_or(0);

        // A foreign epoch with prior progress is a gap: the requester's
        // position refers to a journal that no longer exists.
        if !epoch_matches && request.from_seq > 0 {
            return Ok(FetchJournalResponse {
                epoch: Bytes::copy_from_slice(epoch.as_bytes()),
                oldest_seq,
                entries: Vec::new(),
                next_seq: request.from_seq,
                acknowledged_seq,
                acknowledged_epoch: acknowledged_epoch.clone(),
            });
        }
        if request.from_seq < oldest_seq {
            return Ok(FetchJournalResponse {
                epoch: Bytes::copy_from_slice(epoch.as_bytes()),
                oldest_seq,
                entries: Vec::new(),
                next_seq: request.from_seq,
                acknowledged_seq,
                acknowledged_epoch: acknowledged_epoch.clone(),
            });
        }

        let max_entries = (request.max_entries as usize).clamp(1, MAX_SERVER_ENTRIES);
        let max_bytes = request.max_bytes.clamp(1, MAX_SERVER_BYTES);
        match self
            .journal
            .read_range(request.from_seq, max_entries, max_bytes)
            .map_err(|error| format!("journal read failed: {error}"))?
        {
            JournalRead::Gap { oldest_seq } => Ok(FetchJournalResponse {
                epoch: Bytes::copy_from_slice(epoch.as_bytes()),
                oldest_seq,
                entries: Vec::new(),
                next_seq: request.from_seq,
                acknowledged_seq,
                acknowledged_epoch: acknowledged_epoch.clone(),
            }),
            JournalRead::Entries { entries, next_seq } => Ok(FetchJournalResponse {
                epoch: Bytes::copy_from_slice(epoch.as_bytes()),
                oldest_seq,
                entries: entries
                    .into_iter()
                    .map(|entry| WireJournalEntry {
                        seq: entry.seq,
                        op: u32::from(entry.op.as_u8()),
                        key: entry.key,
                        hlc: Some(oceanfs_core::proto::common::HlcTimestamp {
                            wall_time: entry.hlc.wall_time(),
                            logical: entry.hlc.logical(),
                        }),
                    })
                    .collect(),
                next_seq,
                acknowledged_seq,
                acknowledged_epoch,
            }),
            _ => Err("journal read returned an unknown outcome".to_string()),
        }
    }

    /// Streams the current state of each requested key. Keys absent on
    /// this holder are simply not streamed; supersede records never
    /// travel (the plain-tombstone accessor is used).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let stream = service.metadata_rows(keys);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime (spawns a blocking task).
    pub fn metadata_rows(
        self: &Arc<Self>,
        keys: Vec<Bytes>,
    ) -> tokio_stream::wrappers::ReceiverStream<Result<MetadataRow, Status>> {
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let service = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            for key in keys {
                if started.elapsed() > Duration::from_secs(30) {
                    break;
                }
                let Some((bucket, object_key)) = split_object_row_key(&key) else {
                    continue;
                };
                let bucket = BucketId::new(bucket);
                let object_key = ObjectKey::new(object_key);

                let object_row =
                    match service.metadata_store.get_object_metadata(&bucket, &object_key) {
                        Ok(Some(meta)) => bincode::serialize(&meta).ok().map(|value| MetadataRow {
                            row: Some(metadata_row::Row::Object(ObjectRow {
                                key: key.clone(),
                                value: value.into(),
                            })),
                        }),
                        _ => None,
                    };
                let row = match object_row {
                    Some(row) => Some(row),
                    None => match service.metadata_store.get_tombstone(&bucket, &object_key) {
                        Ok(Some(tombstone)) => {
                            bincode::serialize(&tombstone).ok().map(|value| MetadataRow {
                                row: Some(metadata_row::Row::Deletion(DeletionRow {
                                    key: key.clone(),
                                    value: value.into(),
                                })),
                            })
                        }
                        _ => None,
                    },
                };
                if let Some(row) = row {
                    if sender.blocking_send(Ok(row)).is_err() {
                        break;
                    }
                }
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(receiver)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use oceanfs_core::{MetadataConfig, NodeId};

    use super::*;

    fn request(epoch: &[u8], from_seq: u64, requester: &NodeId) -> FetchJournalRequest {
        FetchJournalRequest {
            epoch: Bytes::copy_from_slice(epoch),
            from_seq,
            max_entries: 16,
            max_bytes: 4096,
            requester_id: Bytes::copy_from_slice(requester.as_str().as_bytes()),
        }
    }

    #[test]
    fn foreign_epoch_pull_is_never_recorded_as_an_ack() {
        let meta_dir = tempfile::tempdir().unwrap();
        let journal_dir = tempfile::tempdir().unwrap();
        let journal =
            Arc::new(MetadataJournal::open(journal_dir.path(), &NodeId::new("node-a")).unwrap());
        let watermarks =
            Arc::new(WatermarkStore::open(&meta_dir.path().join("watermarks")).unwrap());
        let store: Arc<dyn MetadataStore> = Arc::new(
            oceanfs_storage::RocksDbMetadataStore::open(&MetadataConfig {
                data_dir: meta_dir.path().join("db"),
                ..Default::default()
            })
            .unwrap(),
        );
        let service =
            MetadataSyncService::new(Arc::clone(&journal), Arc::clone(&watermarks), store);
        let requester = NodeId::new("node-b");

        // A foreign-epoch high watermark (e.g. the peer restarted with a
        // stale position) must NOT be recorded: it would inflate the trim
        // frontier and over-trim the new journal.
        let foreign = service.fetch_journal(request(&[9u8; 16], 100_000, &requester)).unwrap();
        assert!(foreign.entries.is_empty(), "a foreign epoch with progress is a gap");
        let record = watermarks.get(&requester);
        assert_eq!(record.acked, 0, "a foreign-epoch pull is not an ack");
        assert_eq!(record.acked_epoch, None);

        // A matching-epoch pull with prior progress records the ack.
        let epoch = *journal.epoch().as_bytes();
        service.fetch_journal(request(&epoch, 3, &requester)).unwrap();
        assert_eq!(watermarks.get(&requester).acked, 3);
        assert_eq!(watermarks.get(&requester).acked_epoch, Some(epoch));
    }
}
