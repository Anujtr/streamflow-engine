package partitioning

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// PartitionManager manages partition lifecycle and rebalancing
type PartitionManager struct {
	storage    *storage.Storage
	hashRing   *HashRing
	partitions map[string]*TopicPartitions // topic -> partitions
	mu         sync.RWMutex

	// Metrics
	rebalanceCount   int64
	lastRebalance    time.Time
	partitionMetrics map[string]*PartitionMetrics
	metricsMu        sync.RWMutex
}

// TopicPartitions manages partitions for a specific topic
type TopicPartitions struct {
	TopicName      string
	PartitionCount int32
	HashRing       *HashRing
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// PartitionMetrics tracks metrics for a partition
type PartitionMetrics struct {
	PartitionID    int32     `json:"partition_id"`
	MessageCount   int64     `json:"message_count"`
	BytesStored    int64     `json:"bytes_stored"`
	LastActivity   time.Time `json:"last_activity"`
	ProduceRate    float64   `json:"produce_rate_per_sec"`
	ConsumeRate    float64   `json:"consume_rate_per_sec"`
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(storage *storage.Storage) *PartitionManager {
	return &PartitionManager{
		storage:          storage,
		hashRing:         NewHashRing(DefaultVirtualNodes),
		partitions:       make(map[string]*TopicPartitions),
		partitionMetrics: make(map[string]*PartitionMetrics),
	}
}

// CreateTopicPartitions creates partitions for a topic using consistent hashing
func (pm *PartitionManager) CreateTopicPartitions(topicName string, partitionCount int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[topicName]; exists {
		return fmt.Errorf("topic %s already has partitions", topicName)
	}

	// Create the topic in storage first
	err := pm.storage.CreateTopic(topicName, partitionCount)
	if err != nil {
		return fmt.Errorf("failed to create topic in storage: %v", err)
	}

	// Create hash ring for this topic
	topicHashRing := NewHashRing(DefaultVirtualNodes)
	
	// Add partitions to the hash ring
	for i := int32(0); i < partitionCount; i++ {
		topicHashRing.AddPartition(i)
	}

	pm.partitions[topicName] = &TopicPartitions{
		TopicName:      topicName,
		PartitionCount: partitionCount,
		HashRing:       topicHashRing,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// Initialize metrics for each partition
	for i := int32(0); i < partitionCount; i++ {
		metricKey := fmt.Sprintf("%s-%d", topicName, i)
		pm.partitionMetrics[metricKey] = &PartitionMetrics{
			PartitionID:   i,
			MessageCount:  0,
			BytesStored:   0,
			LastActivity:  time.Now(),
		}
	}

	return nil
}

// GetPartition returns the partition ID for a key in a topic
func (pm *PartitionManager) GetPartition(topicName, key string) (int32, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	topicPartitions, exists := pm.partitions[topicName]
	if !exists {
		return -1, fmt.Errorf("topic %s does not exist", topicName)
	}

	partition := topicPartitions.HashRing.GetPartition(key)
	if partition == -1 {
		return -1, fmt.Errorf("no partitions available for topic %s", topicName)
	}

	return partition, nil
}

// ScalePartitions adds or removes partitions for a topic
func (pm *PartitionManager) ScalePartitions(topicName string, newPartitionCount int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	topicPartitions, exists := pm.partitions[topicName]
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicName)
	}

	currentCount := topicPartitions.PartitionCount
	
	if newPartitionCount == currentCount {
		return nil // No change needed
	}

	if newPartitionCount < currentCount {
		return fmt.Errorf("scaling down partitions not supported (data loss risk)")
	}

	// Scale up: add new partitions
	topic, err := pm.storage.GetTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to get topic: %v", err)
	}

	// Add new partitions to storage
	for i := currentCount; i < newPartitionCount; i++ {
		topic.Partitions[i] = storage.NewPartition(i)
		topicPartitions.HashRing.AddPartition(i)
		
		// Initialize metrics for new partition
		metricKey := fmt.Sprintf("%s-%d", topicName, i)
		pm.partitionMetrics[metricKey] = &PartitionMetrics{
			PartitionID:  i,
			MessageCount: 0,
			BytesStored:  0,
			LastActivity: time.Now(),
		}
	}

	// Update topic info
	topic.NumPartitions = newPartitionCount
	topicPartitions.PartitionCount = newPartitionCount
	topicPartitions.UpdatedAt = time.Now()

	atomic.AddInt64(&pm.rebalanceCount, 1)
	pm.lastRebalance = time.Now()

	return nil
}

// GetTopicPartitions returns partition information for a topic
func (pm *PartitionManager) GetTopicPartitions(topicName string) (*TopicPartitions, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	topicPartitions, exists := pm.partitions[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	// Return a copy to avoid race conditions
	return &TopicPartitions{
		TopicName:      topicPartitions.TopicName,
		PartitionCount: topicPartitions.PartitionCount,
		HashRing:       topicPartitions.HashRing, // Note: sharing hash ring is OK as it has its own mutex
		CreatedAt:      topicPartitions.CreatedAt,
		UpdatedAt:      topicPartitions.UpdatedAt,
	}, nil
}

// UpdatePartitionMetrics updates metrics for a partition
func (pm *PartitionManager) UpdatePartitionMetrics(topicName string, partitionID int32, messageCount, bytesAdded int64) {
	pm.metricsMu.Lock()
	defer pm.metricsMu.Unlock()

	metricKey := fmt.Sprintf("%s-%d", topicName, partitionID)
	metrics, exists := pm.partitionMetrics[metricKey]
	if !exists {
		metrics = &PartitionMetrics{
			PartitionID: partitionID,
		}
		pm.partitionMetrics[metricKey] = metrics
	}

	metrics.MessageCount += messageCount
	metrics.BytesStored += bytesAdded
	metrics.LastActivity = time.Now()
}

// GetPartitionMetrics returns metrics for a specific partition
func (pm *PartitionManager) GetPartitionMetrics(topicName string, partitionID int32) (*PartitionMetrics, error) {
	pm.metricsMu.RLock()
	defer pm.metricsMu.RUnlock()

	metricKey := fmt.Sprintf("%s-%d", topicName, partitionID)
	metrics, exists := pm.partitionMetrics[metricKey]
	if !exists {
		return nil, fmt.Errorf("metrics not found for partition %d in topic %s", partitionID, topicName)
	}

	// Return a copy
	return &PartitionMetrics{
		PartitionID:    metrics.PartitionID,
		MessageCount:   metrics.MessageCount,
		BytesStored:    metrics.BytesStored,
		LastActivity:   metrics.LastActivity,
		ProduceRate:    metrics.ProduceRate,
		ConsumeRate:    metrics.ConsumeRate,
	}, nil
}

// GetAllPartitionMetrics returns metrics for all partitions in a topic
func (pm *PartitionManager) GetAllPartitionMetrics(topicName string) ([]*PartitionMetrics, error) {
	topicPartitions, err := pm.GetTopicPartitions(topicName)
	if err != nil {
		return nil, err
	}

	pm.metricsMu.RLock()
	defer pm.metricsMu.RUnlock()

	metrics := make([]*PartitionMetrics, 0, topicPartitions.PartitionCount)
	
	for i := int32(0); i < topicPartitions.PartitionCount; i++ {
		metricKey := fmt.Sprintf("%s-%d", topicName, i)
		if metric, exists := pm.partitionMetrics[metricKey]; exists {
			// Add a copy
			metrics = append(metrics, &PartitionMetrics{
				PartitionID:    metric.PartitionID,
				MessageCount:   metric.MessageCount,
				BytesStored:    metric.BytesStored,
				LastActivity:   metric.LastActivity,
				ProduceRate:    metric.ProduceRate,
				ConsumeRate:    metric.ConsumeRate,
			})
		}
	}

	return metrics, nil
}

// GetManagerStats returns overall partition manager statistics
func (pm *PartitionManager) GetManagerStats() ManagerStats {
	pm.mu.RLock()
	pm.metricsMu.RLock()
	defer pm.mu.RUnlock()
	defer pm.metricsMu.RUnlock()

	return ManagerStats{
		TopicCount:       len(pm.partitions),
		TotalPartitions:  pm.getTotalPartitionCount(),
		RebalanceCount:   atomic.LoadInt64(&pm.rebalanceCount),
		LastRebalance:    pm.lastRebalance,
		PartitionMetrics: len(pm.partitionMetrics),
	}
}

func (pm *PartitionManager) getTotalPartitionCount() int {
	total := 0
	for _, tp := range pm.partitions {
		total += int(tp.PartitionCount)
	}
	return total
}

// ManagerStats contains partition manager statistics
type ManagerStats struct {
	TopicCount       int       `json:"topic_count"`
	TotalPartitions  int       `json:"total_partitions"`
	RebalanceCount   int64     `json:"rebalance_count"`
	LastRebalance    time.Time `json:"last_rebalance"`
	PartitionMetrics int       `json:"partition_metrics_count"`
}