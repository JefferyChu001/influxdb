//! JOIN optimizer for selecting the best JOIN strategy

use crate::error::Result;

/// JOIN strategy selection
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    /// Broadcast the small table to all nodes
    Broadcast {
        small_table: String,
        large_table: String,
        broadcast_side: BroadcastSide,
    },
    /// Shuffle both tables by JOIN key
    Shuffle {
        partition_count: usize,
    },
    /// Data is already co-located by JOIN key
    CoLocated,
}

/// Which side to broadcast in a broadcast JOIN
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BroadcastSide {
    Left,
    Right,
}

/// Table statistics for JOIN optimization
#[derive(Debug, Clone, Copy)]
pub struct TableStatistics {
    pub row_count: u64,
    pub estimated_size_bytes: u64,
}

/// JOIN optimizer
#[derive(Debug, Clone, Copy)]
pub struct JoinOptimizer {
    /// Threshold for broadcast JOIN (in bytes)
    broadcast_threshold: u64,
    /// Maximum number of partitions for shuffle JOIN
    max_partitions: usize,
}

impl JoinOptimizer {
    pub fn new(broadcast_threshold: u64, max_partitions: usize) -> Self {
        Self {
            broadcast_threshold,
            max_partitions,
        }
    }

    /// Select the optimal JOIN strategy based on table statistics
    pub fn optimize_join(
        &self,
        left_table: &str,
        right_table: &str,
        left_stats: &TableStatistics,
        right_stats: &TableStatistics,
    ) -> Result<JoinStrategy> {
        let left_size = left_stats.estimated_size_bytes;
        let right_size = right_stats.estimated_size_bytes;

        // If left table is small enough, broadcast it
        if left_size < self.broadcast_threshold {
            return Ok(JoinStrategy::Broadcast {
                small_table: left_table.to_string(),
                large_table: right_table.to_string(),
                broadcast_side: BroadcastSide::Left,
            });
        }

        // If right table is small enough, broadcast it
        if right_size < self.broadcast_threshold {
            return Ok(JoinStrategy::Broadcast {
                small_table: right_table.to_string(),
                large_table: left_table.to_string(),
                broadcast_side: BroadcastSide::Right,
            });
        }

        // Both tables are large, use shuffle JOIN
        let partition_count = self.calculate_optimal_partitions(left_size + right_size);
        Ok(JoinStrategy::Shuffle { partition_count })
    }

    /// Calculate optimal number of partitions based on data size
    fn calculate_optimal_partitions(&self, total_size: u64) -> usize {
        // Target partition size: 256MB
        const TARGET_PARTITION_SIZE: u64 = 256 * 1024 * 1024;

        let partitions = (total_size / TARGET_PARTITION_SIZE).max(1) as usize;

        // Limit to max_partitions
        partitions.min(self.max_partitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_left() {
        let optimizer = JoinOptimizer::new(100 * 1024 * 1024, 256);

        let left_stats = TableStatistics {
            row_count: 1000,
            estimated_size_bytes: 50 * 1024 * 1024, // 50MB
        };

        let right_stats = TableStatistics {
            row_count: 1_000_000,
            estimated_size_bytes: 1024 * 1024 * 1024, // 1GB
        };

        let strategy = optimizer
            .optimize_join("left_table", "right_table", &left_stats, &right_stats)
            .unwrap();

        match strategy {
            JoinStrategy::Broadcast {
                broadcast_side, ..
            } => {
                assert_eq!(broadcast_side, BroadcastSide::Left);
            }
            _ => panic!("Expected Broadcast strategy"),
        }
    }

    #[test]
    fn test_shuffle_join() {
        let optimizer = JoinOptimizer::new(100 * 1024 * 1024, 256);

        let left_stats = TableStatistics {
            row_count: 1_000_000,
            estimated_size_bytes: 512 * 1024 * 1024, // 512MB
        };

        let right_stats = TableStatistics {
            row_count: 2_000_000,
            estimated_size_bytes: 1024 * 1024 * 1024, // 1GB
        };

        let strategy = optimizer
            .optimize_join("left_table", "right_table", &left_stats, &right_stats)
            .unwrap();

        match strategy {
            JoinStrategy::Shuffle { partition_count } => {
                assert!(partition_count > 0);
                assert!(partition_count <= 256);
            }
            _ => panic!("Expected Shuffle strategy"),
        }
    }
}

