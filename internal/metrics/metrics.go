package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceMetrics tracks system performance metrics
type PerformanceMetrics struct {
	// Throughput metrics
	MessagesProduced     int64 `json:"messages_produced"`
	MessagesConsumed     int64 `json:"messages_consumed"`
	BatchesProcessed     int64 `json:"batches_processed"`
	
	// Latency metrics
	ProduceLatency       LatencyMetrics `json:"produce_latency"`
	ConsumeLatency       LatencyMetrics `json:"consume_latency"`
	BatchProcessLatency  LatencyMetrics `json:"batch_process_latency"`
	
	// Error metrics
	ProduceErrors        int64 `json:"produce_errors"`
	ConsumeErrors        int64 `json:"consume_errors"`
	BatchErrors          int64 `json:"batch_errors"`
	
	// Storage metrics
	PebbleMetrics        PebbleMetrics `json:"pebble_metrics"`
	
	// Partition metrics
	PartitionMetrics     map[string]*PartitionMetrics `json:"partition_metrics"`
	partitionMu          sync.RWMutex
	
	startTime            time.Time
}

// LatencyMetrics tracks latency percentiles
type LatencyMetrics struct {
	Count         int64         `json:"count"`
	TotalDuration time.Duration `json:"total_duration"`
	AvgLatency    time.Duration `json:"avg_latency"`
	P50Latency    time.Duration `json:"p50_latency"`
	P99Latency    time.Duration `json:"p99_latency"`
	MaxLatency    time.Duration `json:"max_latency"`
	
	// Ring buffer for latency samples
	samples       []time.Duration
	sampleIndex   int
	mu            sync.RWMutex
}

// PebbleMetrics tracks Pebble database performance
type PebbleMetrics struct {
	WriteAmplification   float64 `json:"write_amplification"`
	CacheHitRatio       float64 `json:"cache_hit_ratio"`
	CompactionCount     int64   `json:"compaction_count"`
	MemTableCount       int64   `json:"memtable_count"`
	L0FileCount         int64   `json:"l0_file_count"`
	TotalBytesStored    int64   `json:"total_bytes_stored"`
	BatchWriteCount     int64   `json:"batch_write_count"`
	BatchCommitLatency  time.Duration `json:"batch_commit_latency"`
}

// PartitionMetrics tracks per-partition performance
type PartitionMetrics struct {
	TopicName           string    `json:"topic_name"`
	PartitionID         int32     `json:"partition_id"`
	MessageCount        int64     `json:"message_count"`
	BytesStored         int64     `json:"bytes_stored"`
	ProduceRate         float64   `json:"produce_rate_per_sec"`
	ConsumeRate         float64   `json:"consume_rate_per_sec"`
	LastActivity        time.Time `json:"last_activity"`
	CachedOffsetHits    int64     `json:"cached_offset_hits"`
	OffsetCacheMisses   int64     `json:"offset_cache_misses"`
}

// NewPerformanceMetrics creates a new performance metrics tracker
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		ProduceLatency:      NewLatencyMetrics(1000), // Track last 1000 samples
		ConsumeLatency:      NewLatencyMetrics(1000),
		BatchProcessLatency: NewLatencyMetrics(500),
		PartitionMetrics:    make(map[string]*PartitionMetrics),
		startTime:           time.Now(),
	}
}

// NewLatencyMetrics creates a new latency metrics tracker
func NewLatencyMetrics(sampleSize int) LatencyMetrics {
	return LatencyMetrics{
		samples: make([]time.Duration, sampleSize),
	}
}

// RecordProduceLatency records a produce operation latency
func (pm *PerformanceMetrics) RecordProduceLatency(latency time.Duration) {
	atomic.AddInt64(&pm.MessagesProduced, 1)
	pm.ProduceLatency.Record(latency)
}

// RecordConsumeLatency records a consume operation latency
func (pm *PerformanceMetrics) RecordConsumeLatency(latency time.Duration, messageCount int) {
	atomic.AddInt64(&pm.MessagesConsumed, int64(messageCount))
	pm.ConsumeLatency.Record(latency)
}

// RecordBatchProcessLatency records batch processing latency
func (pm *PerformanceMetrics) RecordBatchProcessLatency(latency time.Duration, batchSize int) {
	atomic.AddInt64(&pm.BatchesProcessed, 1)
	pm.BatchProcessLatency.Record(latency)
}

// IncrementProduceErrors increments produce error count
func (pm *PerformanceMetrics) IncrementProduceErrors() {
	atomic.AddInt64(&pm.ProduceErrors, 1)
}

// IncrementConsumeErrors increments consume error count
func (pm *PerformanceMetrics) IncrementConsumeErrors() {
	atomic.AddInt64(&pm.ConsumeErrors, 1)
}

// IncrementBatchErrors increments batch error count
func (pm *PerformanceMetrics) IncrementBatchErrors() {
	atomic.AddInt64(&pm.BatchErrors, 1)
}

// UpdatePartitionMetrics updates metrics for a specific partition
func (pm *PerformanceMetrics) UpdatePartitionMetrics(topicName string, partitionID int32, messageCount, bytesStored int64) {
	pm.partitionMu.Lock()
	defer pm.partitionMu.Unlock()
	
	key := getPartitionKey(topicName, partitionID)
	if metrics, exists := pm.PartitionMetrics[key]; exists {
		metrics.MessageCount = messageCount
		metrics.BytesStored = bytesStored
		metrics.LastActivity = time.Now()
	} else {
		pm.PartitionMetrics[key] = &PartitionMetrics{
			TopicName:    topicName,
			PartitionID:  partitionID,
			MessageCount: messageCount,
			BytesStored:  bytesStored,
			LastActivity: time.Now(),
		}
	}
}

// Record records a latency sample
func (lm *LatencyMetrics) Record(latency time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// Update running statistics
	atomic.AddInt64(&lm.Count, 1)
	lm.TotalDuration += latency
	lm.AvgLatency = time.Duration(int64(lm.TotalDuration) / lm.Count)
	
	if latency > lm.MaxLatency {
		lm.MaxLatency = latency
	}
	
	// Add to ring buffer for percentile calculation
	lm.samples[lm.sampleIndex] = latency
	lm.sampleIndex = (lm.sampleIndex + 1) % len(lm.samples)
	
	// Calculate percentiles (simplified implementation)
	if lm.Count%100 == 0 { // Recalculate every 100 samples to avoid overhead
		lm.calculatePercentiles()
	}
}

// calculatePercentiles calculates P50 and P99 latencies from samples
func (lm *LatencyMetrics) calculatePercentiles() {
	// Create a copy of samples for sorting
	samples := make([]time.Duration, len(lm.samples))
	copy(samples, lm.samples)
	
	// Simple bubble sort for small sample sizes
	for i := 0; i < len(samples)-1; i++ {
		for j := 0; j < len(samples)-i-1; j++ {
			if samples[j] > samples[j+1] {
				samples[j], samples[j+1] = samples[j+1], samples[j]
			}
		}
	}
	
	// Calculate percentiles
	if len(samples) > 0 {
		p50Index := len(samples) * 50 / 100
		p99Index := len(samples) * 99 / 100
		if p99Index >= len(samples) {
			p99Index = len(samples) - 1
		}
		
		lm.P50Latency = samples[p50Index]
		lm.P99Latency = samples[p99Index]
	}
}

// GetSnapshot returns a snapshot of current metrics
func (pm *PerformanceMetrics) GetSnapshot() *PerformanceMetrics {
	pm.partitionMu.RLock()
	defer pm.partitionMu.RUnlock()
	
	snapshot := &PerformanceMetrics{
		MessagesProduced:    atomic.LoadInt64(&pm.MessagesProduced),
		MessagesConsumed:    atomic.LoadInt64(&pm.MessagesConsumed),
		BatchesProcessed:    atomic.LoadInt64(&pm.BatchesProcessed),
		ProduceErrors:       atomic.LoadInt64(&pm.ProduceErrors),
		ConsumeErrors:       atomic.LoadInt64(&pm.ConsumeErrors),
		BatchErrors:         atomic.LoadInt64(&pm.BatchErrors),
		ProduceLatency:      pm.copyLatencyMetrics(&pm.ProduceLatency),
		ConsumeLatency:      pm.copyLatencyMetrics(&pm.ConsumeLatency),
		BatchProcessLatency: pm.copyLatencyMetrics(&pm.BatchProcessLatency),
		PebbleMetrics:       pm.PebbleMetrics,
		PartitionMetrics:    make(map[string]*PartitionMetrics),
		startTime:           pm.startTime,
	}
	
	// Copy partition metrics
	for key, metrics := range pm.PartitionMetrics {
		snapshot.PartitionMetrics[key] = &PartitionMetrics{
			TopicName:         metrics.TopicName,
			PartitionID:       metrics.PartitionID,
			MessageCount:      metrics.MessageCount,
			BytesStored:       metrics.BytesStored,
			ProduceRate:       metrics.ProduceRate,
			ConsumeRate:       metrics.ConsumeRate,
			LastActivity:      metrics.LastActivity,
			CachedOffsetHits:  metrics.CachedOffsetHits,
			OffsetCacheMisses: metrics.OffsetCacheMisses,
		}
	}
	
	return snapshot
}

// copyLatencyMetrics creates a deep copy of LatencyMetrics without copying the mutex
func (pm *PerformanceMetrics) copyLatencyMetrics(src *LatencyMetrics) LatencyMetrics {
	src.mu.RLock()
	defer src.mu.RUnlock()
	
	// Create a new samples slice
	samplesCopy := make([]time.Duration, len(src.samples))
	copy(samplesCopy, src.samples)
	
	return LatencyMetrics{
		Count:         atomic.LoadInt64(&src.Count),
		TotalDuration: src.TotalDuration,
		AvgLatency:    src.AvgLatency,
		P50Latency:    src.P50Latency,
		P99Latency:    src.P99Latency,
		MaxLatency:    src.MaxLatency,
		samples:       samplesCopy,
		sampleIndex:   src.sampleIndex,
		// Don't copy the mutex - it will be initialized as zero value
	}
}

// CalculateRates calculates throughput rates based on elapsed time
func (pm *PerformanceMetrics) CalculateRates() {
	elapsed := time.Since(pm.startTime).Seconds()
	if elapsed == 0 {
		return
	}
	
	pm.partitionMu.Lock()
	defer pm.partitionMu.Unlock()
	
	for _, metrics := range pm.PartitionMetrics {
		metrics.ProduceRate = float64(metrics.MessageCount) / elapsed
		// ConsumeRate would need additional tracking
	}
}

// getPartitionKey creates a unique key for partition metrics
func getPartitionKey(topicName string, partitionID int32) string {
	return fmt.Sprintf("%s:%d", topicName, partitionID)
}