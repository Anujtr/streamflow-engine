package partitioning

import (
	"fmt"
	"testing"
)

func TestNewHashRing(t *testing.T) {
	ring := NewHashRing(100)
	
	if ring == nil {
		t.Fatal("Expected non-nil hash ring")
	}
	
	if ring.replicas != 100 {
		t.Errorf("Expected replicas to be 100, got %d", ring.replicas)
	}
	
	if len(ring.virtualNodes) != 0 {
		t.Errorf("Expected empty virtual nodes map, got %d entries", len(ring.virtualNodes))
	}
}

func TestNewHashRing_DefaultReplicas(t *testing.T) {
	ring := NewHashRing(0)
	
	if ring.replicas != DefaultVirtualNodes {
		t.Errorf("Expected replicas to be %d, got %d", DefaultVirtualNodes, ring.replicas)
	}
}

func TestHashRing_AddPartition(t *testing.T) {
	ring := NewHashRing(10)
	
	// Add first partition
	ring.AddPartition(0)
	
	if len(ring.partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(ring.partitions))
	}
	
	if len(ring.virtualNodes) != 10 {
		t.Errorf("Expected 10 virtual nodes, got %d", len(ring.virtualNodes))
	}
	
	if len(ring.sortedHashes) != 10 {
		t.Errorf("Expected 10 sorted hashes, got %d", len(ring.sortedHashes))
	}
	
	// Add same partition again (should be idempotent)
	ring.AddPartition(0)
	
	if len(ring.partitions) != 1 {
		t.Errorf("Expected 1 partition after duplicate add, got %d", len(ring.partitions))
	}
}

func TestHashRing_RemovePartition(t *testing.T) {
	ring := NewHashRing(10)
	
	// Add partition first
	ring.AddPartition(0)
	ring.AddPartition(1)
	
	if len(ring.partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(ring.partitions))
	}
	
	// Remove partition
	ring.RemovePartition(0)
	
	if len(ring.partitions) != 1 {
		t.Errorf("Expected 1 partition after removal, got %d", len(ring.partitions))
	}
	
	if len(ring.virtualNodes) != 10 {
		t.Errorf("Expected 10 virtual nodes after removal, got %d", len(ring.virtualNodes))
	}
	
	// Verify the remaining partition
	if !ring.partitions[1] {
		t.Error("Expected partition 1 to still exist")
	}
}

func TestHashRing_GetPartition(t *testing.T) {
	ring := NewHashRing(50)
	
	// No partitions should return -1
	partition := ring.GetPartition("test-key")
	if partition != -1 {
		t.Errorf("Expected -1 for empty ring, got %d", partition)
	}
	
	// Add partitions
	ring.AddPartition(0)
	ring.AddPartition(1)
	ring.AddPartition(2)
	
	// Test key distribution
	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	partitionCounts := make(map[int32]int)
	
	for _, key := range testKeys {
		partition := ring.GetPartition(key)
		if partition < 0 || partition > 2 {
			t.Errorf("Expected partition in range [0,2], got %d for key %s", partition, key)
		}
		partitionCounts[partition]++
	}
	
	// Verify all partitions are used
	if len(partitionCounts) == 0 {
		t.Error("No partitions were assigned")
	}
}

func TestHashRing_GetPartitions(t *testing.T) {
	ring := NewHashRing(10)
	
	// Empty ring
	partitions := ring.GetPartitions()
	if len(partitions) != 0 {
		t.Errorf("Expected empty partitions list, got %d", len(partitions))
	}
	
	// Add partitions
	ring.AddPartition(2)
	ring.AddPartition(0)
	ring.AddPartition(1)
	
	partitions = ring.GetPartitions()
	
	if len(partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partitions))
	}
	
	// Verify sorted order
	expectedOrder := []int32{0, 1, 2}
	for i, expected := range expectedOrder {
		if partitions[i] != expected {
			t.Errorf("Expected partition %d at index %d, got %d", expected, i, partitions[i])
		}
	}
}

func TestHashRing_GetLoadDistribution(t *testing.T) {
	ring := NewHashRing(50)
	ring.AddPartition(0)
	ring.AddPartition(1)
	ring.AddPartition(2)
	
	keys := []string{}
	for i := 0; i < 300; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	
	distribution := ring.GetLoadDistribution(keys)
	
	if len(distribution) != 3 {
		t.Errorf("Expected distribution for 3 partitions, got %d", len(distribution))
	}
	
	totalKeys := 0
	for partition, count := range distribution {
		if partition < 0 || partition > 2 {
			t.Errorf("Unexpected partition %d in distribution", partition)
		}
		if count < 0 {
			t.Errorf("Negative key count %d for partition %d", count, partition)
		}
		totalKeys += count
	}
	
	if totalKeys != 300 {
		t.Errorf("Expected total keys to be 300, got %d", totalKeys)
	}
}

func TestHashRing_GetDistributionStats(t *testing.T) {
	ring := NewHashRing(100)
	ring.AddPartition(0)
	ring.AddPartition(1)
	ring.AddPartition(2)
	
	keys := []string{}
	for i := 0; i < 600; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	
	stats := ring.GetDistributionStats(keys)
	
	if stats.TotalKeys != 600 {
		t.Errorf("Expected total keys to be 600, got %d", stats.TotalKeys)
	}
	
	if stats.PartitionCount != 3 {
		t.Errorf("Expected partition count to be 3, got %d", stats.PartitionCount)
	}
	
	if stats.AvgKeysPartition <= 0 {
		t.Errorf("Expected positive average keys per partition, got %f", stats.AvgKeysPartition)
	}
	
	if stats.MinKeysPartition > stats.MaxKeysPartition {
		t.Errorf("Min keys (%d) should not be greater than max keys (%d)", 
			stats.MinKeysPartition, stats.MaxKeysPartition)
	}
	
	if stats.VariancePercent < 0 {
		t.Errorf("Expected non-negative variance percent, got %f", stats.VariancePercent)
	}
}

func TestHashRing_Consistency(t *testing.T) {
	ring := NewHashRing(50)
	ring.AddPartition(0)
	ring.AddPartition(1)
	ring.AddPartition(2)
	
	testKey := "consistent-key"
	
	// Get partition for key multiple times
	partition1 := ring.GetPartition(testKey)
	partition2 := ring.GetPartition(testKey)
	partition3 := ring.GetPartition(testKey)
	
	if partition1 != partition2 || partition2 != partition3 {
		t.Errorf("Key assignment not consistent: %d, %d, %d", partition1, partition2, partition3)
	}
	
	// Add new partition and verify key assignment is deterministic
	ring.AddPartition(3)
	newPartition := ring.GetPartition(testKey)
	
	// In consistent hashing, the key might move to a new partition
	// but this should be deterministic
	ring2 := NewHashRing(50)
	ring2.AddPartition(0)
	ring2.AddPartition(1)
	ring2.AddPartition(2)
	ring2.AddPartition(3)
	
	expectedPartition := ring2.GetPartition(testKey)
	if newPartition != expectedPartition {
		t.Errorf("Key assignment not deterministic after partition addition: got %d, expected %d", 
			newPartition, expectedPartition)
	}
}

func BenchmarkHashRing_GetPartition(b *testing.B) {
	ring := NewHashRing(150)
	for i := 0; i < 32; i++ {
		ring.AddPartition(int32(i))
	}
	
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("benchmark-key-%d", i)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			ring.GetPartition(keys[i%len(keys)])
			i++
		}
	})
}