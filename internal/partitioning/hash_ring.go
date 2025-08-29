package partitioning

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the default number of virtual nodes per partition
	DefaultVirtualNodes = 150
)

// HashRing implements consistent hashing with virtual nodes
type HashRing struct {
	virtualNodes map[uint32]int32 // virtual node hash -> partition ID
	sortedHashes []uint32         // sorted list of virtual node hashes
	partitions   map[int32]bool   // available partitions
	replicas     int              // number of virtual nodes per partition
	mu           sync.RWMutex
}

// NewHashRing creates a new consistent hash ring
func NewHashRing(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = DefaultVirtualNodes
	}

	return &HashRing{
		virtualNodes: make(map[uint32]int32),
		sortedHashes: make([]uint32, 0),
		partitions:   make(map[int32]bool),
		replicas:     replicas,
	}
}

// AddPartition adds a partition to the hash ring
func (hr *HashRing) AddPartition(partitionID int32) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.partitions[partitionID] {
		return // Already exists
	}

	hr.partitions[partitionID] = true

	// Add virtual nodes for this partition
	for i := 0; i < hr.replicas; i++ {
		hash := hr.hash(fmt.Sprintf("%d:%d", partitionID, i))
		hr.virtualNodes[hash] = partitionID
		hr.sortedHashes = append(hr.sortedHashes, hash)
	}

	// Keep hashes sorted
	sort.Slice(hr.sortedHashes, func(i, j int) bool {
		return hr.sortedHashes[i] < hr.sortedHashes[j]
	})
}

// RemovePartition removes a partition from the hash ring
func (hr *HashRing) RemovePartition(partitionID int32) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.partitions[partitionID] {
		return // Doesn't exist
	}

	delete(hr.partitions, partitionID)

	// Remove virtual nodes for this partition
	newHashes := make([]uint32, 0, len(hr.sortedHashes))
	for _, hash := range hr.sortedHashes {
		if hr.virtualNodes[hash] != partitionID {
			newHashes = append(newHashes, hash)
		} else {
			delete(hr.virtualNodes, hash)
		}
	}

	hr.sortedHashes = newHashes
}

// GetPartition returns the partition ID for a given key using consistent hashing
func (hr *HashRing) GetPartition(key string) int32 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return -1 // No partitions available
	}

	hash := hr.hash(key)

	// Binary search for the first hash >= key hash
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= hash
	})

	// Wrap around if we reached the end
	if idx == len(hr.sortedHashes) {
		idx = 0
	}

	return hr.virtualNodes[hr.sortedHashes[idx]]
}

// GetPartitions returns all partition IDs in the ring
func (hr *HashRing) GetPartitions() []int32 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	partitions := make([]int32, 0, len(hr.partitions))
	for partitionID := range hr.partitions {
		partitions = append(partitions, partitionID)
	}

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})

	return partitions
}

// GetPartitionCount returns the number of partitions
func (hr *HashRing) GetPartitionCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.partitions)
}

// GetLoadDistribution returns the load distribution across partitions for a set of keys
func (hr *HashRing) GetLoadDistribution(keys []string) map[int32]int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	distribution := make(map[int32]int)
	
	// Initialize all partitions with 0
	for partitionID := range hr.partitions {
		distribution[partitionID] = 0
	}

	// Count keys per partition
	for _, key := range keys {
		if partition := hr.GetPartition(key); partition != -1 {
			distribution[partition]++
		}
	}

	return distribution
}

// GetDistributionStats returns statistics about load distribution
func (hr *HashRing) GetDistributionStats(keys []string) DistributionStats {
	distribution := hr.GetLoadDistribution(keys)
	
	if len(distribution) == 0 {
		return DistributionStats{}
	}

	var total, min, max int
	min = int(^uint(0) >> 1) // Max int
	
	for _, count := range distribution {
		total += count
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
	}

	avg := float64(total) / float64(len(distribution))
	variance := float64(max-min) / avg * 100 // Percentage variance

	return DistributionStats{
		TotalKeys:        total,
		PartitionCount:   len(distribution),
		MinKeysPartition: min,
		MaxKeysPartition: max,
		AvgKeysPartition: avg,
		VariancePercent:  variance,
		Distribution:     distribution,
	}
}

// hash computes SHA-1 hash of the input string
func (hr *HashRing) hash(input string) uint32 {
	h := sha1.Sum([]byte(input))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

// DistributionStats contains statistics about load distribution
type DistributionStats struct {
	TotalKeys        int            `json:"total_keys"`
	PartitionCount   int            `json:"partition_count"`
	MinKeysPartition int            `json:"min_keys_partition"`
	MaxKeysPartition int            `json:"max_keys_partition"`
	AvgKeysPartition float64        `json:"avg_keys_partition"`
	VariancePercent  float64        `json:"variance_percent"`
	Distribution     map[int32]int  `json:"distribution"`
}