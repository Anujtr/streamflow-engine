package metrics

import (
	"testing"
	"time"
)

func TestNewPerformanceMetrics(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	if metrics == nil {
		t.Fatal("Expected non-nil metrics")
	}
	
	if metrics.PartitionMetrics == nil {
		t.Error("Expected non-nil PartitionMetrics map")
	}
	
	if metrics.MessagesProduced != 0 {
		t.Error("Expected MessagesProduced to be 0 initially")
	}
}

func TestRecordProduceLatency(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	latency := 10 * time.Millisecond
	metrics.RecordProduceLatency(latency)
	
	if metrics.MessagesProduced != 1 {
		t.Errorf("Expected MessagesProduced to be 1, got %d", metrics.MessagesProduced)
	}
	
	if metrics.ProduceLatency.Count != 1 {
		t.Errorf("Expected ProduceLatency.Count to be 1, got %d", metrics.ProduceLatency.Count)
	}
	
	if metrics.ProduceLatency.TotalDuration != latency {
		t.Errorf("Expected TotalDuration to be %v, got %v", latency, metrics.ProduceLatency.TotalDuration)
	}
}

func TestRecordConsumeLatency(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	latency := 5 * time.Millisecond
	messageCount := 10
	metrics.RecordConsumeLatency(latency, messageCount)
	
	if metrics.MessagesConsumed != int64(messageCount) {
		t.Errorf("Expected MessagesConsumed to be %d, got %d", messageCount, metrics.MessagesConsumed)
	}
	
	if metrics.ConsumeLatency.Count != 1 {
		t.Errorf("Expected ConsumeLatency.Count to be 1, got %d", metrics.ConsumeLatency.Count)
	}
}

func TestRecordBatchProcessLatency(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	latency := 2 * time.Millisecond
	batchSize := 50
	metrics.RecordBatchProcessLatency(latency, batchSize)
	
	if metrics.BatchesProcessed != 1 {
		t.Errorf("Expected BatchesProcessed to be 1, got %d", metrics.BatchesProcessed)
	}
	
	if metrics.BatchProcessLatency.Count != 1 {
		t.Errorf("Expected BatchProcessLatency.Count to be 1, got %d", metrics.BatchProcessLatency.Count)
	}
}

func TestIncrementErrors(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	metrics.IncrementProduceErrors()
	metrics.IncrementConsumeErrors()
	metrics.IncrementBatchErrors()
	
	if metrics.ProduceErrors != 1 {
		t.Errorf("Expected ProduceErrors to be 1, got %d", metrics.ProduceErrors)
	}
	
	if metrics.ConsumeErrors != 1 {
		t.Errorf("Expected ConsumeErrors to be 1, got %d", metrics.ConsumeErrors)
	}
	
	if metrics.BatchErrors != 1 {
		t.Errorf("Expected BatchErrors to be 1, got %d", metrics.BatchErrors)
	}
}

func TestGetSnapshot(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	// Add some data
	metrics.RecordProduceLatency(10 * time.Millisecond)
	metrics.RecordConsumeLatency(5 * time.Millisecond, 5)
	metrics.IncrementProduceErrors()
	metrics.UpdatePartitionMetrics("test-topic", 0, 100, 1024)
	
	// Get snapshot
	snapshot := metrics.GetSnapshot()
	
	if snapshot == nil {
		t.Fatal("Expected non-nil snapshot")
	}
	
	// Verify data is copied correctly
	if snapshot.MessagesProduced != metrics.MessagesProduced {
		t.Errorf("Expected MessagesProduced %d, got %d", metrics.MessagesProduced, snapshot.MessagesProduced)
	}
	
	if snapshot.MessagesConsumed != metrics.MessagesConsumed {
		t.Errorf("Expected MessagesConsumed %d, got %d", metrics.MessagesConsumed, snapshot.MessagesConsumed)
	}
	
	if snapshot.ProduceErrors != metrics.ProduceErrors {
		t.Errorf("Expected ProduceErrors %d, got %d", metrics.ProduceErrors, snapshot.ProduceErrors)
	}
	
	// Verify latency metrics are copied correctly
	if snapshot.ProduceLatency.Count != 1 {
		t.Errorf("Expected ProduceLatency.Count to be 1, got %d", snapshot.ProduceLatency.Count)
	}
	
	// Verify partition metrics are copied
	if len(snapshot.PartitionMetrics) != 1 {
		t.Errorf("Expected 1 partition metric, got %d", len(snapshot.PartitionMetrics))
	}
	
	key := "test-topic:0"
	partitionMetric, exists := snapshot.PartitionMetrics[key]
	if !exists {
		t.Error("Expected partition metric for test-topic:0")
	} else {
		if partitionMetric.MessageCount != 100 {
			t.Errorf("Expected MessageCount 100, got %d", partitionMetric.MessageCount)
		}
		if partitionMetric.BytesStored != 1024 {
			t.Errorf("Expected BytesStored 1024, got %d", partitionMetric.BytesStored)
		}
	}
}

func TestUpdatePartitionMetrics(t *testing.T) {
	metrics := NewPerformanceMetrics()
	
	topicName := "test-topic"
	partitionID := int32(1)
	messageCount := int64(500)
	bytesStored := int64(2048)
	
	metrics.UpdatePartitionMetrics(topicName, partitionID, messageCount, bytesStored)
	
	key := "test-topic:1"
	partitionMetric, exists := metrics.PartitionMetrics[key]
	if !exists {
		t.Fatal("Expected partition metric to be created")
	}
	
	if partitionMetric.TopicName != topicName {
		t.Errorf("Expected TopicName %s, got %s", topicName, partitionMetric.TopicName)
	}
	
	if partitionMetric.PartitionID != partitionID {
		t.Errorf("Expected PartitionID %d, got %d", partitionID, partitionMetric.PartitionID)
	}
	
	if partitionMetric.MessageCount != messageCount {
		t.Errorf("Expected MessageCount %d, got %d", messageCount, partitionMetric.MessageCount)
	}
	
	if partitionMetric.BytesStored != bytesStored {
		t.Errorf("Expected BytesStored %d, got %d", bytesStored, partitionMetric.BytesStored)
	}
	
	if partitionMetric.LastActivity.IsZero() {
		t.Error("Expected LastActivity to be set")
	}
}

func TestLatencyMetricsRecord(t *testing.T) {
	metrics := NewLatencyMetrics(10)
	
	// Record some latencies
	latencies := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
	}
	
	for _, latency := range latencies {
		metrics.Record(latency)
	}
	
	if metrics.Count != int64(len(latencies)) {
		t.Errorf("Expected Count %d, got %d", len(latencies), metrics.Count)
	}
	
	expectedTotal := 6 * time.Millisecond
	if metrics.TotalDuration != expectedTotal {
		t.Errorf("Expected TotalDuration %v, got %v", expectedTotal, metrics.TotalDuration)
	}
	
	expectedAvg := 2 * time.Millisecond
	if metrics.AvgLatency != expectedAvg {
		t.Errorf("Expected AvgLatency %v, got %v", expectedAvg, metrics.AvgLatency)
	}
	
	if metrics.MaxLatency != 3*time.Millisecond {
		t.Errorf("Expected MaxLatency %v, got %v", 3*time.Millisecond, metrics.MaxLatency)
	}
}