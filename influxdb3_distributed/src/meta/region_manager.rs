//! Region manager for tracking region distribution.

use crate::common::{NodeId, PartitionRange, RegionId, RegionInfo, RegionStatus};
use crate::error::{DistributedError, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Manages region distribution across the cluster.
///
/// The RegionManager tracks which regions exist, where they are located,
/// and provides methods for region assignment and lookup.
#[derive(Debug)]
pub struct RegionManager {
    /// All regions indexed by RegionId
    regions: DashMap<RegionId, RegionInfo>,

    /// Regions indexed by table (database.table -> [RegionId])
    table_regions: DashMap<String, Vec<RegionId>>,

    /// Regions indexed by node (NodeId -> [RegionId])
    node_regions: DashMap<NodeId, Vec<RegionId>>,

    /// Next region ID to assign
    next_region_id: AtomicU64,

    /// Configuration for region management
    config: RegionManagerConfig,
}

/// Configuration for the RegionManager.
#[derive(Debug, Clone, Copy)]
pub struct RegionManagerConfig {
    /// Default number of regions for new tables
    pub default_num_regions: usize,

    /// Maximum regions per node
    pub max_regions_per_node: usize,
}

impl Default for RegionManagerConfig {
    fn default() -> Self {
        Self {
            default_num_regions: 4,
            max_regions_per_node: 100,
        }
    }
}

impl RegionManager {
    /// Create a new RegionManager.
    pub fn new(config: RegionManagerConfig) -> Self {
        Self {
            regions: DashMap::new(),
            table_regions: DashMap::new(),
            node_regions: DashMap::new(),
            next_region_id: AtomicU64::new(1),
            config,
        }
    }

    /// Create a new RegionManager with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(RegionManagerConfig::default())
    }

    /// Generate the next region ID.
    fn next_region_id(&self) -> RegionId {
        RegionId::new(self.next_region_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Create table key from database and table name.
    fn table_key(database: &str, table: &str) -> String {
        format!("{}.{}", database, table)
    }

    /// Create regions for a new table.
    ///
    /// This creates the specified number of regions with hash-based partitioning
    /// and assigns them to available nodes.
    pub fn create_table_regions(
        &self,
        database: &str,
        table: &str,
        num_regions: usize,
        available_nodes: &[NodeId],
    ) -> Result<Vec<RegionInfo>> {
        if available_nodes.is_empty() {
            return Err(DistributedError::ClusterNotInitialized);
        }

        let table_key = Self::table_key(database, table);

        // Check if table already has regions
        if self.table_regions.contains_key(&table_key) {
            return Err(DistributedError::InvalidRequest(format!(
                "Table {}.{} already has regions",
                database, table
            )));
        }

        let num_regions = if num_regions == 0 {
            self.config.default_num_regions
        } else {
            num_regions
        };

        let mut created_regions = Vec::with_capacity(num_regions);
        let mut region_ids = Vec::with_capacity(num_regions);

        for i in 0..num_regions {
            let region_id = self.next_region_id();
            let node_id = available_nodes[i % available_nodes.len()];
            let partition_range = PartitionRange::for_hash_bucket(i as u16, num_regions as u16);

            let region_info = RegionInfo::new(
                region_id,
                database.to_string(),
                table.to_string(),
                node_id,
                partition_range,
            );

            // Add to regions map
            self.regions.insert(region_id, region_info.clone());

            // Add to node's region list
            self.node_regions
                .entry(node_id)
                .or_insert_with(Vec::new)
                .push(region_id);

            region_ids.push(region_id);
            created_regions.push(region_info);
        }

        // Add to table regions map
        self.table_regions.insert(table_key, region_ids);

        Ok(created_regions)
    }

    /// Get all regions for a table.
    pub fn get_table_regions(&self, database: &str, table: &str) -> Result<Vec<RegionInfo>> {
        let table_key = Self::table_key(database, table);

        let region_ids = self.table_regions.get(&table_key).ok_or_else(|| {
            DistributedError::TableNotFound {
                database: database.to_string(),
                table: table.to_string(),
            }
        })?;

        let regions: Vec<RegionInfo> = region_ids
            .iter()
            .filter_map(|id| self.regions.get(id).map(|r| r.clone()))
            .collect();

        Ok(regions)
    }

    /// Get all regions for a database.
    pub fn get_database_regions(&self, database: &str) -> Vec<RegionInfo> {
        let prefix = format!("{}.", database);
        let mut regions = Vec::new();

        for entry in self.table_regions.iter() {
            if entry.key().starts_with(&prefix) {
                for region_id in entry.value() {
                    if let Some(region) = self.regions.get(region_id) {
                        regions.push(region.clone());
                    }
                }
            }
        }

        regions
    }

    /// Get a specific region by ID.
    pub fn get_region(&self, region_id: RegionId) -> Result<RegionInfo> {
        self.regions
            .get(&region_id)
            .map(|r| r.clone())
            .ok_or_else(|| DistributedError::RegionNotFound {
                region_id: region_id.get(),
            })
    }

    /// Get all regions assigned to a node.
    pub fn get_node_regions(&self, node_id: NodeId) -> Vec<RegionInfo> {
        self.node_regions
            .get(&node_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.regions.get(id).map(|r| r.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Assign a region to a node.
    pub fn assign_region(&self, region_id: RegionId, new_node_id: NodeId) -> Result<()> {
        let mut region = self.regions.get_mut(&region_id).ok_or_else(|| {
            DistributedError::RegionNotFound {
                region_id: region_id.get(),
            }
        })?;

        let old_node_id = region.node_id;

        // Remove from old node's list
        if let Some(mut old_regions) = self.node_regions.get_mut(&old_node_id) {
            old_regions.retain(|id| *id != region_id);
        }

        // Add to new node's list
        self.node_regions
            .entry(new_node_id)
            .or_insert_with(Vec::new)
            .push(region_id);

        // Update region's node assignment and increment epoch
        region.node_id = new_node_id;
        region.epoch += 1;

        Ok(())
    }

    /// Update region status.
    pub fn update_region_status(
        &self,
        region_id: RegionId,
        status: RegionStatus,
    ) -> Result<()> {
        let mut region = self.regions.get_mut(&region_id).ok_or_else(|| {
            DistributedError::RegionNotFound {
                region_id: region_id.get(),
            }
        })?;

        region.status = status;
        Ok(())
    }

    /// Find regions that match a time and hash range (for query routing).
    pub fn find_matching_regions(
        &self,
        database: &str,
        table: &str,
        time_start_ns: Option<i64>,
        time_end_ns: Option<i64>,
        hash_value: Option<u16>,
    ) -> Result<Vec<RegionInfo>> {
        let regions = self.get_table_regions(database, table)?;

        let matching: Vec<RegionInfo> = regions
            .into_iter()
            .filter(|r| {
                // Filter by time range
                let time_matches =
                    r.partition_range.overlaps_time_range(time_start_ns, time_end_ns);

                // Filter by hash if provided
                let hash_matches = hash_value
                    .map(|h| r.partition_range.contains_hash(h))
                    .unwrap_or(true);

                time_matches && hash_matches && r.can_serve_query()
            })
            .collect();

        Ok(matching)
    }

    /// Get count of regions per node (for load balancing).
    pub fn get_node_region_counts(&self) -> HashMap<NodeId, usize> {
        self.node_regions
            .iter()
            .map(|entry| (*entry.key(), entry.value().len()))
            .collect()
    }

    /// Find the node with the least regions (for new region assignment).
    pub fn find_least_loaded_node(&self, candidates: &[NodeId]) -> Option<NodeId> {
        let counts = self.get_node_region_counts();

        candidates
            .iter()
            .min_by_key(|id| counts.get(id).copied().unwrap_or(0))
            .copied()
    }

    /// Remove all regions for a node (called when node goes offline).
    pub fn remove_node_regions(&self, node_id: NodeId) -> Vec<RegionId> {
        if let Some((_, region_ids)) = self.node_regions.remove(&node_id) {
            // Mark regions as offline
            for region_id in &region_ids {
                if let Some(mut region) = self.regions.get_mut(region_id) {
                    region.status = RegionStatus::Offline;
                }
            }
            region_ids
        } else {
            Vec::new()
        }
    }

    /// Get all tables in a database.
    pub fn get_database_tables(&self, database: &str) -> Vec<String> {
        let prefix = format!("{}.", database);

        self.table_regions
            .iter()
            .filter_map(|entry| {
                if entry.key().starts_with(&prefix) {
                    Some(entry.key().strip_prefix(&prefix)?.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get total number of regions.
    pub fn total_regions(&self) -> usize {
        self.regions.len()
    }

    /// Get total number of tables.
    pub fn total_tables(&self) -> usize {
        self.table_regions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> Vec<NodeId> {
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)]
    }

    #[test]
    fn test_create_table_regions() {
        let manager = RegionManager::with_defaults();
        let nodes = create_test_nodes();

        let regions = manager
            .create_table_regions("mydb", "cpu", 6, &nodes)
            .unwrap();

        assert_eq!(regions.len(), 6);

        // Check regions are distributed across nodes
        let node1_count = regions.iter().filter(|r| r.node_id == nodes[0]).count();
        let node2_count = regions.iter().filter(|r| r.node_id == nodes[1]).count();
        let node3_count = regions.iter().filter(|r| r.node_id == nodes[2]).count();

        assert_eq!(node1_count, 2);
        assert_eq!(node2_count, 2);
        assert_eq!(node3_count, 2);
    }

    #[test]
    fn test_get_table_regions() {
        let manager = RegionManager::with_defaults();
        let nodes = create_test_nodes();

        manager
            .create_table_regions("mydb", "cpu", 4, &nodes)
            .unwrap();

        let regions = manager.get_table_regions("mydb", "cpu").unwrap();
        assert_eq!(regions.len(), 4);

        // Non-existent table should error
        let result = manager.get_table_regions("mydb", "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_assign_region() {
        let manager = RegionManager::with_defaults();
        let nodes = create_test_nodes();

        let regions = manager
            .create_table_regions("mydb", "cpu", 1, &nodes)
            .unwrap();
        let region_id = regions[0].region_id;

        // Initially assigned to node 1
        assert_eq!(regions[0].node_id, nodes[0]);

        // Reassign to node 2
        manager.assign_region(region_id, nodes[1]).unwrap();

        let region = manager.get_region(region_id).unwrap();
        assert_eq!(region.node_id, nodes[1]);
        assert_eq!(region.epoch, 1); // Epoch should be incremented
    }

    #[test]
    fn test_find_matching_regions() {
        let manager = RegionManager::with_defaults();
        let nodes = create_test_nodes();

        manager
            .create_table_regions("mydb", "cpu", 4, &nodes)
            .unwrap();

        // Without hash filter should return all regions
        let regions = manager
            .find_matching_regions("mydb", "cpu", None, None, None)
            .unwrap();
        assert_eq!(regions.len(), 4);

        // With hash filter should return matching region
        let regions = manager
            .find_matching_regions("mydb", "cpu", None, None, Some(0))
            .unwrap();
        assert_eq!(regions.len(), 1);
    }

    #[test]
    fn test_find_least_loaded_node() {
        let manager = RegionManager::with_defaults();
        let nodes = create_test_nodes();

        // Create tables with different region counts on nodes
        manager
            .create_table_regions("db1", "t1", 3, &[nodes[0]])
            .unwrap();
        manager
            .create_table_regions("db1", "t2", 2, &[nodes[1]])
            .unwrap();

        // Node 2 has 0 regions, should be least loaded
        let least_loaded = manager.find_least_loaded_node(&nodes);
        assert_eq!(least_loaded, Some(nodes[2]));
    }
}
