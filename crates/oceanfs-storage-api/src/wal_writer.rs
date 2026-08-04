//! WAL writer trait — write-ahead log append/sync operations.
//!
//! Abstracts sequential WAL writes so that durability workers and
//! coordinators are decoupled from the concrete WAL backend.

use crate::error::Error;

/// Write-ahead log writer for crash recovery.
///
/// # Examples
///
/// ```
/// use oceanfs_storage_api::WalWriter;
/// use oceanfs_storage_api::error::Error;
///
/// struct MyWal;
///
/// #[async_trait::async_trait]
/// impl WalWriter for MyWal {
///     async fn append(&self, _entry_data: &[u8]) -> Result<u64, Error> {
///         Ok(0)
///     }
///
///     async fn truncate(&self, _position: u64) -> Result<(), Error> {
///         Ok(())
///     }
///
///     async fn sync(&self) -> Result<(), Error> {
///         Ok(())
///     }
///
///     async fn global_position(&self) -> u64 {
///         0
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait WalWriter: Send + Sync {
    /// Appends an entry to the WAL.
    ///
    /// `entry_data` is the serialized WAL entry payload.
    /// Returns the global WAL position of the newly written entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails or the sync group shuts down.
    async fn append(&self, entry_data: &[u8]) -> Result<u64, Error>;

    /// Truncates the WAL at the given position.
    ///
    /// Entries at or after `position` are discarded. Used after segment
    /// sealing to reclaim WAL space.
    ///
    /// # Errors
    ///
    /// Returns an error if the truncation fails.
    async fn truncate(&self, position: u64) -> Result<(), Error>;

    /// Force-syncs the current WAL file to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the fsync fails.
    async fn sync(&self) -> Result<(), Error>;

    /// Returns the current global WAL position.
    async fn global_position(&self) -> u64;
}
