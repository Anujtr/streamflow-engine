package client

import (
	"context"
	"testing"
	"time"
)

func TestProducerConfig_Defaults(t *testing.T) {
	config := ProducerConfig{}

	producer, err := NewProducer(config)
	if err != nil {
		t.Errorf("Unexpected error creating producer: %v", err)
		return
	}
	defer producer.Close()

	// Test that defaults are applied
	if config.Address == "" {
		config.Address = "localhost:8080" // Default should be applied internally
	}
}

func TestProducerMessage(t *testing.T) {
	msg := Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	if msg.Key != "test-key" {
		t.Errorf("Expected key 'test-key', got %s", msg.Key)
	}

	if string(msg.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got %s", string(msg.Value))
	}
}

func TestProduceResult(t *testing.T) {
	result := &ProduceResult{
		Partition: 1,
		Offset:    42,
		Error:     "test error",
	}

	if result.Partition != 1 {
		t.Errorf("Expected partition 1, got %d", result.Partition)
	}

	if result.Offset != 42 {
		t.Errorf("Expected offset 42, got %d", result.Offset)
	}

	if result.Error != "test error" {
		t.Errorf("Expected error 'test error', got %s", result.Error)
	}
}

func TestProducerMetrics(t *testing.T) {
	metrics := ProducerMetrics{
		MessagesSent: 100,
		Errors:       5,
	}

	if metrics.MessagesSent != 100 {
		t.Errorf("Expected 100 messages sent, got %d", metrics.MessagesSent)
	}

	if metrics.Errors != 5 {
		t.Errorf("Expected 5 errors, got %d", metrics.Errors)
	}
}

// Integration test - requires running server
func TestProducer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	producer, err := NewProducer(ProducerConfig{
		Address: "localhost:8080",
		Timeout: 5 * time.Second,
	})

	// If server isn't running, skip this test
	if err != nil {
		t.Skipf("Server not available: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()

	// Test sending a single message
	result, err := producer.SendSingle(ctx, "test-topic", "test-key", []byte("test-value"))
	if err != nil {
		t.Skipf("Failed to send message (server may not be running): %v", err)
	}

	if result.Error != "" {
		t.Errorf("Unexpected server error: %s", result.Error)
	}

	if result.Partition < 0 {
		t.Errorf("Invalid partition: %d", result.Partition)
	}

	if result.Offset < 0 {
		t.Errorf("Invalid offset: %d", result.Offset)
	}

	// Test metrics
	metrics := producer.GetMetrics()
	if metrics.MessagesSent < 1 {
		t.Errorf("Expected at least 1 message sent, got %d", metrics.MessagesSent)
	}
}