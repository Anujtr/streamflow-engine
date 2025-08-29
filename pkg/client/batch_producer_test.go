package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/grpc"
)

// mockMessageServiceClient implements pb.MessageServiceClient for testing
type mockMessageServiceClient struct {
	pb.MessageServiceClient
	requests      []*pb.ProduceRequest
	shouldError   bool
	responseDelay time.Duration
}

func (m *mockMessageServiceClient) Produce(ctx context.Context, req *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
	if m.responseDelay > 0 {
		time.Sleep(m.responseDelay)
	}
	
	if m.shouldError {
		return nil, fmt.Errorf("mock error")
	}
	
	m.requests = append(m.requests, req)
	
	// Create successful response
	results := make([]*pb.ProduceResult, len(req.Messages))
	for i := range req.Messages {
		results[i] = &pb.ProduceResult{
			Partition: int32(i % 4), // Simple partition assignment
			Offset:    int64(i),
			Error:     "",
		}
	}
	
	return &pb.ProduceResponse{
		Results: results,
	}, nil
}

func TestNewBatchProducer(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize:  100,
		BatchTimeout:  50 * time.Millisecond,
		FlushInterval: 25 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	if producer == nil {
		t.Fatal("Expected non-nil batch producer")
	}
	
	if producer.config.MaxBatchSize != 100 {
		t.Errorf("Expected max batch size 100, got %d", producer.config.MaxBatchSize)
	}
	
	if producer.config.BatchTimeout != 50*time.Millisecond {
		t.Errorf("Expected batch timeout 50ms, got %v", producer.config.BatchTimeout)
	}
}

func TestBatchProducer_DefaultConfig(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{} // Empty config to test defaults
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	if producer.config.MaxBatchSize != 1000 {
		t.Errorf("Expected default max batch size 1000, got %d", producer.config.MaxBatchSize)
	}
	
	if producer.config.MaxBatchBytes != 1024*1024 {
		t.Errorf("Expected default max batch bytes 1MB, got %d", producer.config.MaxBatchBytes)
	}
	
	if producer.config.BatchTimeout != 100*time.Millisecond {
		t.Errorf("Expected default batch timeout 100ms, got %v", producer.config.BatchTimeout)
	}
}

func TestBatchProducer_SendAsync(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize: 5,
		BatchTimeout: 100 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send a message
	err := producer.SendAsync(ctx, "test-topic", "key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to send async message: %v", err)
	}
	
	// Verify batch was created
	producer.batchMu.RLock()
	batch, exists := producer.batches["test-topic"]
	producer.batchMu.RUnlock()
	
	if !exists {
		t.Error("Expected batch to be created for test-topic")
	}
	
	batch.mu.Lock()
	if len(batch.messages) != 1 {
		t.Errorf("Expected 1 message in batch, got %d", len(batch.messages))
	}
	batch.mu.Unlock()
}

func TestBatchProducer_SendAsync_InvalidTopic(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send message with empty topic
	err := producer.SendAsync(ctx, "", "key1", []byte("value1"))
	if err == nil {
		t.Error("Expected error for empty topic")
	}
}

func TestBatchProducer_AutoFlush(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize: 3, // Small batch size for quick triggering
		BatchTimeout: 1 * time.Second, // Long timeout to test size-based flush
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send messages to trigger auto-flush
	for i := 0; i < 4; i++ {
		err := producer.SendAsync(ctx, "test-topic", fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}
	
	// Give some time for batching to process
	time.Sleep(100 * time.Millisecond)
	
	// Should have triggered at least one flush due to batch size limit
	if len(mockClient.requests) == 0 {
		t.Error("Expected at least one batch to be flushed")
	}
	
	// First batch should have 3 messages (due to MaxBatchSize=3)
	if len(mockClient.requests[0].Messages) != 3 {
		t.Errorf("Expected first batch to have 3 messages, got %d", len(mockClient.requests[0].Messages))
	}
}

func TestBatchProducer_TimeoutFlush(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize:  100, // Large batch size
		BatchTimeout:  50 * time.Millisecond, // Short timeout for quick test
		FlushInterval: 25 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send a single message (won't trigger size-based flush)
	err := producer.SendAsync(ctx, "test-topic", "key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
	
	// Wait for timeout-based flush
	time.Sleep(100 * time.Millisecond)
	
	// Should have flushed due to timeout
	if len(mockClient.requests) == 0 {
		t.Error("Expected batch to be flushed due to timeout")
	}
	
	if len(mockClient.requests[0].Messages) != 1 {
		t.Errorf("Expected batch to have 1 message, got %d", len(mockClient.requests[0].Messages))
	}
}

func TestBatchProducer_ManualFlush(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize: 100, // Large batch size
		BatchTimeout: 1 * time.Second, // Long timeout
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send messages without triggering auto-flush
	for i := 0; i < 5; i++ {
		err := producer.SendAsync(ctx, "test-topic", fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Errorf("Failed to send message %d: %v", i, err)
		}
	}
	
	// Manual flush
	err := producer.Flush(ctx)
	if err != nil {
		t.Errorf("Failed to flush: %v", err)
	}
	
	// Should have flushed
	if len(mockClient.requests) == 0 {
		t.Error("Expected batch to be flushed manually")
	}
	
	if len(mockClient.requests[0].Messages) != 5 {
		t.Errorf("Expected batch to have 5 messages, got %d", len(mockClient.requests[0].Messages))
	}
}

func TestBatchProducer_MultipleTopics(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize: 10,
		BatchTimeout: 100 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send messages to different topics
	topics := []string{"topic-1", "topic-2", "topic-3"}
	for _, topic := range topics {
		for i := 0; i < 3; i++ {
			err := producer.SendAsync(ctx, topic, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("value%d", i)))
			if err != nil {
				t.Errorf("Failed to send message to %s: %v", topic, err)
			}
		}
	}
	
	// Manual flush
	err := producer.Flush(ctx)
	if err != nil {
		t.Errorf("Failed to flush: %v", err)
	}
	
	// Should have 3 batches (one per topic)
	if len(mockClient.requests) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(mockClient.requests))
	}
	
	// Verify each batch has correct topic and message count
	topicCounts := make(map[string]int)
	for _, req := range mockClient.requests {
		topicCounts[req.Topic] = len(req.Messages)
	}
	
	for _, topic := range topics {
		if count, exists := topicCounts[topic]; !exists || count != 3 {
			t.Errorf("Expected 3 messages for topic %s, got %d", topic, count)
		}
	}
}

func TestBatchProducer_ErrorHandling(t *testing.T) {
	mockClient := &mockMessageServiceClient{
		shouldError: true, // Simulate errors
	}
	config := BatchProducerConfig{
		MaxBatchSize:  2,
		RetryAttempts: 1, // Single retry
		RetryBackoff:  10 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Send messages that will trigger flush and error
	err := producer.SendAsync(ctx, "test-topic", "key1", []byte("value1"))
	if err != nil {
		t.Errorf("SendAsync should not fail: %v", err)
	}
	
	err = producer.SendAsync(ctx, "test-topic", "key2", []byte("value2"))
	if err != nil {
		t.Errorf("SendAsync should not fail: %v", err)
	}
	
	// Trigger flush that will fail
	err = producer.Flush(ctx)
	if err == nil {
		t.Error("Expected flush to fail due to mock error")
	}
	
	// Check that error count increased
	metrics := producer.GetMetrics()
	if metrics.Errors == 0 {
		t.Error("Expected error count to be greater than 0")
	}
}

func TestBatchProducer_GetMetrics(t *testing.T) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize: 2,
		BatchTimeout: 50 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	// Initial metrics
	initialMetrics := producer.GetMetrics()
	if initialMetrics.MessagesSent != 0 {
		t.Errorf("Expected 0 messages sent initially, got %d", initialMetrics.MessagesSent)
	}
	
	// Send messages and flush
	err := producer.SendAsync(ctx, "test-topic", "key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
	
	err = producer.SendAsync(ctx, "test-topic", "key2", []byte("value2"))
	if err != nil {
		t.Errorf("Failed to send message: %v", err)
	}
	
	// Wait for auto-flush
	time.Sleep(100 * time.Millisecond)
	
	// Check updated metrics
	metrics := producer.GetMetrics()
	if metrics.MessagesSent != 2 {
		t.Errorf("Expected 2 messages sent, got %d", metrics.MessagesSent)
	}
	
	if metrics.BatchesSent != 1 {
		t.Errorf("Expected 1 batch sent, got %d", metrics.BatchesSent)
	}
	
	if metrics.BytesProduced == 0 {
		t.Error("Expected positive bytes produced")
	}
}

func BenchmarkBatchProducer_SendAsync(b *testing.B) {
	mockClient := &mockMessageServiceClient{}
	config := BatchProducerConfig{
		MaxBatchSize:  1000,
		BatchTimeout:  1 * time.Second,
		FlushInterval: 100 * time.Millisecond,
	}
	
	producer := NewBatchProducer(mockClient, config)
	defer producer.Close(context.Background())
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			producer.SendAsync(ctx, "benchmark-topic", key, value)
			i++
		}
	})
}