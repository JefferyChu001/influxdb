//! Region types for data partitioning and distribution.

use super::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

/// Unique identifier for a region (data partition).
///
/// A region is a unit of data distribution. Each table can be split into
/// multiple regions, and each region is assigned to a Datanode.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RegionId(u64);

impl RegionId {
    /// Create a new RegionId from a u64 value.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the underlying u64 value.
    pub const fn get(&self) -> u64 {
        self.0
    }

    /// Create a RegionId from bytes (big-endian).
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Convert to bytes (big-endian).
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl From<u64> for RegionId {
    fn from(id: u64) -> Self {
        Self::new(id)
    }
}

impl From<RegionId> for u64 {
    fn from(id: RegionId) -> Self {
        id.0
    }
}

impl fmt::Debug for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RegionId({})", self.0)
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for RegionId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(RegionId::new)
    }
}

/// The partition range for a region.
///
/// This defines the range of data that belongs to this region,
/// based on time range and/or hash partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRange {
    /// Start timestamp (inclusive) in nanoseconds, None means unbounded
    pub time_start_ns: Option<i64>,

    /// End timestamp (exclusive) in nanoseconds, None means unbounded
    pub time_end_ns: Option<i64>,

    /// Hash partition start (inclusive), range 0-65535
    pub hash_start: u16,

    /// Hash partition end (exclusive), range 0-65536
    pub hash_end: u16,
}

impl PartitionRange {
    /// Maximum hash value (exclusive). Hash range is [0, MAX_HASH).
    const MAX_HASH: u32 = 65536;

    /// Create a new PartitionRange covering the full hash range.
    pub fn full() -> Self {
        Self {
            time_start_ns: None,
            time_end_ns: None,
            hash_start: 0,
            hash_end: u16::MAX, // Use MAX to represent "up to 65536"
        }
    }

    /// Create a partition range for a specific hash bucket.
    ///
    /// # Arguments
    /// * `bucket` - The bucket number (0-based)
    /// * `total_buckets` - Total number of buckets
    pub fn for_hash_bucket(bucket: u16, total_buckets: u16) -> Self {
        let bucket_size = Self::MAX_HASH / total_buckets as u32;
        let start = (bucket as u32 * bucket_size) as u16;
        let end = if bucket == total_buckets - 1 {
            u16::MAX // Last bucket extends to u16::MAX
        } else {
            ((bucket as u32 + 1) * bucket_size) as u16
        };

        Self {
            time_start_ns: None,
            time_end_ns: None,
            hash_start: start,
            hash_end: end,
        }
    }

    /// Create a partition range with time bounds.
    pub fn with_time_range(mut self, start_ns: Option<i64>, end_ns: Option<i64>) -> Self {
        self.time_start_ns = start_ns;
        self.time_end_ns = end_ns;
        self
    }

    /// Check if a hash value falls within this partition.
    pub fn contains_hash(&self, hash: u16) -> bool {
        hash >= self.hash_start && hash < self.hash_end
    }

    /// Check if a timestamp falls within this partition's time range.
    pub fn contains_time(&self, timestamp_ns: i64) -> bool {
        let after_start = self.time_start_ns.map_or(true, |s| timestamp_ns >= s);
        let before_end = self.time_end_ns.map_or(true, |e| timestamp_ns < e);
        after_start && before_end
    }

    /// Check if this partition overlaps with a time range.
    pub fn overlaps_time_range(&self, start_ns: Option<i64>, end_ns: Option<i64>) -> bool {
        // If query has no bounds, it overlaps with everything
        let query_start = start_ns.unwrap_or(i64::MIN);
        let query_end = end_ns.unwrap_or(i64::MAX);

        let partition_start = self.time_start_ns.unwrap_or(i64::MIN);
        let partition_end = self.time_end_ns.unwrap_or(i64::MAX);

        query_start < partition_end && query_end > partition_start
    }

    /// Check if this partition overlaps with a hash range.
    pub fn overlaps_hash_range(&self, hash_start: u16, hash_end: u16) -> bool {
        self.hash_start < hash_end && self.hash_end > hash_start
    }
}

impl Default for PartitionRange {
    fn default() -> Self {
        Self::full()
    }
}

/// Complete information about a region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionInfo {
    /// Unique identifier for this region
    pub region_id: RegionId,

    /// The database this region belongs to
    pub database: String,

    /// The table this region belongs to
    pub table: String,

    /// The node currently hosting this region
    pub node_id: NodeId,

    /// The partition range for this region
    pub partition_range: PartitionRange,

    /// Current status of this region
    pub status: RegionStatus,

    /// Epoch number, incremented on each region reassignment
    pub epoch: u64,
}

impl RegionInfo {
    /// Create a new RegionInfo
    pub fn new(
        region_id: RegionId,
        database: String,
        table: String,
        node_id: NodeId,
        partition_range: PartitionRange,
    ) -> Self {
        Self {
            region_id,
            database,
            table,
            node_id,
            partition_range,
            status: RegionStatus::Active,
            epoch: 0,
        }
    }

    /// Check if this region is active
    pub fn is_active(&self) -> bool {
        matches!(self.status, RegionStatus::Active)
    }

    /// Check if this region can serve queries
    pub fn can_serve_query(&self) -> bool {
        matches!(
            self.status,
            RegionStatus::Active | RegionStatus::ReadOnly | RegionStatus::Migrating
        )
    }

    /// Check if this region can accept writes
    pub fn can_accept_write(&self) -> bool {
        matches!(self.status, RegionStatus::Active)
    }

    /// Check if a data point with the given hash and timestamp belongs to this region
    pub fn contains(&self, hash: u16, timestamp_ns: i64) -> bool {
        self.partition_range.contains_hash(hash) && self.partition_range.contains_time(timestamp_ns)
    }
}

/// Status of a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is active and can serve reads and writes
    Active,

    /// Region is read-only, typically during migration
    ReadOnly,

    /// Region is being migrated to another node
    Migrating,

    /// Region is offline
    Offline,

    /// Region is being created
    Creating,

    /// Region is being deleted
    Deleting,
}

impl std::fmt::Display for RegionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionStatus::Active => write!(f, "Active"),
            RegionStatus::ReadOnly => write!(f, "ReadOnly"),
            RegionStatus::Migrating => write!(f, "Migrating"),
            RegionStatus::Offline => write!(f, "Offline"),
            RegionStatus::Creating => write!(f, "Creating"),
            RegionStatus::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Compute the hash bucket for a given partition key.
///
/// Uses the xxhash algorithm for consistent hashing.
pub fn compute_partition_hash(partition_key: &[u8]) -> u16 {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(partition_key);
    (hasher.finish() % 65536) as u16
}

/// Compute the hash bucket from a string partition key.
pub fn compute_partition_hash_from_str(partition_key: &str) -> u16 {
    compute_partition_hash(partition_key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_id_creation() {
        let id = RegionId::new(42);
        assert_eq!(id.get(), 42);
    }

    #[test]
    fn test_partition_range_hash_bucket() {
        // Create 4 buckets
        let r0 = PartitionRange::for_hash_bucket(0, 4);
        let r1 = PartitionRange::for_hash_bucket(1, 4);
        let r2 = PartitionRange::for_hash_bucket(2, 4);
        let r3 = PartitionRange::for_hash_bucket(3, 4);

        // Check ranges don't overlap and cover full space
        assert_eq!(r0.hash_start, 0);
        assert_eq!(r0.hash_end, r1.hash_start);
        assert_eq!(r1.hash_end, r2.hash_start);
        assert_eq!(r2.hash_end, r3.hash_start);
        assert_eq!(r3.hash_end, u16::MAX); // Last bucket ends at u16::MAX
    }

    #[test]
    fn test_partition_range_contains_hash() {
        let range = PartitionRange::for_hash_bucket(1, 4);
        let bucket_size: u16 = (65536u32 / 4) as u16;

        // Should not contain values in other buckets
        assert!(!range.contains_hash(0));
        assert!(!range.contains_hash(bucket_size - 1));

        // Should contain values in its bucket
        assert!(range.contains_hash(bucket_size));
        assert!(range.contains_hash(bucket_size * 2 - 1));

        // Should not contain values after its bucket
        assert!(!range.contains_hash(bucket_size * 2));
    }

    #[test]
    fn test_partition_range_time_overlap() {
        let range = PartitionRange::full().with_time_range(Some(100), Some(200));

        // Overlapping ranges
        assert!(range.overlaps_time_range(Some(50), Some(150)));
        assert!(range.overlaps_time_range(Some(150), Some(250)));
        assert!(range.overlaps_time_range(Some(50), Some(250)));
        assert!(range.overlaps_time_range(Some(100), Some(200)));

        // Non-overlapping ranges
        assert!(!range.overlaps_time_range(Some(200), Some(300)));
        assert!(!range.overlaps_time_range(Some(0), Some(100)));

        // Unbounded query overlaps everything
        assert!(range.overlaps_time_range(None, None));
    }

    // #[test]
    // fn test_compute_partition_hash() {
    //     let hash1 = compute_partition_hash_from_str("server1");
    //     let hash2 = compute_partition_hash_from_str("server2");

    //     // Same key should produce same hash
    //     assert_eq!(hash1, compute_partition_hash_from_str("server1"));

    //     // Different keys should (usually) produce different hashes
    //     assert_ne!(hash1, hash2);

    //     // Hash should be in valid range
    //     assert!(hash1 < 65536);
    //     assert!(hash2 < 65536);
    // }

    // #[test]
    // fn test_region_info_contains() {
    //     let region = RegionInfo::new(
    //         RegionId::new(1),
    //         "mydb".to_string(),
    //         "cpu".to_string(),
    //         NodeId::new(1),
    //         PartitionRange::for_hash_bucket(0, 4).with_time_range(Some(100), Some(200)),
    //     );

    //     // Should contain points in the right bucket and time range
    //     assert!(region.contains(0, 150));

    //     // Should not contain points outside time range
    //     assert!(!region.contains(0, 50));
    //     assert!(!region.contains(0, 250));

    //     // Should not contain points outside hash bucket
    //     let other_bucket_hash = 65536 / 4; // First hash in bucket 1
    //     assert!(!region.contains(other_bucket_hash, 150));
    // }
}
