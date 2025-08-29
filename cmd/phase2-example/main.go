package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Anujtr/streamflow-engine/pkg/client"
)

func main() {
	fmt.Println("🚀 StreamFlow Engine Phase 2 Demo")
	fmt.Println("====================================")
	
	// Create client
	c, err := client.NewClient(client.Config{
		Address: "localhost:8080",
		Timeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// Check server health
	health, err := c.Health(ctx)
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	
	fmt.Printf("✅ Server Status: %s (Version: %s)\n", health.Status, health.Version)
	fmt.Printf("📊 Server Metrics: %+v\n\n", health.Metrics)

	// Demo 1: High-throughput batch production
	fmt.Println("🔥 Demo 1: High-Throughput Batch Production")
	fmt.Println("--------------------------------------------")
	
	err = demoBatchProduction(ctx, c)
	if err != nil {
		log.Printf("Batch production demo failed: %v", err)
	}

	// Demo 2: Consistent hashing visualization
	fmt.Println("\n📊 Demo 2: Consistent Hashing Distribution")
	fmt.Println("-------------------------------------------")
	
	err = demoConsistentHashing(ctx, c)
	if err != nil {
		log.Printf("Consistent hashing demo failed: %v", err)
	}

	// Demo 3: Consumer groups (simulated)
	fmt.Println("\n👥 Demo 3: Consumer Group Coordination")
	fmt.Println("---------------------------------------")
	
	err = demoConsumerGroups(ctx, c)
	if err != nil {
		log.Printf("Consumer groups demo failed: %v", err)
	}

	// Final metrics
	fmt.Println("\n📈 Final Performance Metrics")
	fmt.Println("-----------------------------")
	
	finalHealth, err := c.Health(ctx)
	if err != nil {
		log.Printf("Final health check failed: %v", err)
	} else {
		fmt.Printf("Messages Produced: %s\n", finalHealth.Metrics["messages_produced"])
		fmt.Printf("Messages Consumed: %s\n", finalHealth.Metrics["messages_consumed"])
		fmt.Printf("Topics Count: %s\n", finalHealth.Metrics["topics_count"])
	}
	
	fmt.Println("\n🎉 Phase 2 Demo Complete!")
}

func demoBatchProduction(ctx context.Context, c *client.Client) error {
	// Create batch producer (would use the new BatchProducer in real implementation)
	producer, err := c.NewProducer()
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	topic := "phase2-batch-topic"
	
	fmt.Println("📦 Producing 1000 messages in batches...")
	start := time.Now()
	
	// Simulate batch production by sending many messages quickly
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		value := fmt.Sprintf("High-throughput message %d - Phase 2 batch processing", i)
		
		_, err := producer.SendSingle(ctx, topic, key, []byte(value))
		if err != nil {
			return fmt.Errorf("failed to send message %d: %v", i, err)
		}
		
		// Show progress every 100 messages
		if (i+1)%100 == 0 {
			fmt.Printf("  📈 Sent %d messages\n", i+1)
		}
	}
	
	duration := time.Since(start)
	throughput := float64(1000) / duration.Seconds()
	
	fmt.Printf("✅ Produced 1000 messages in %v\n", duration)
	fmt.Printf("🚀 Throughput: %.0f messages/second\n", throughput)
	
	return nil
}

func demoConsistentHashing(ctx context.Context, c *client.Client) error {
	producer, err := c.NewProducer()
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	defer producer.Close()

	consumer, err := c.NewConsumer()
	if err != nil {
		return fmt.Errorf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	topic := "phase2-hash-topic"
	
	// Send messages with different key patterns to show distribution
	keyPatterns := []string{
		"user-1001", "user-1002", "user-1003", "user-1004", "user-1005",
		"order-A001", "order-A002", "order-A003", "order-A004", "order-A005",
		"session-X1", "session-X2", "session-X3", "session-X4", "session-X5",
	}
	
	fmt.Println("🔑 Sending messages with different key patterns...")
	
	for _, key := range keyPatterns {
		value := fmt.Sprintf("Message for key %s - demonstrating consistent hashing", key)
		result, err := producer.SendSingle(ctx, topic, key, []byte(value))
		if err != nil {
			return fmt.Errorf("failed to send message for key %s: %v", key, err)
		}
		
		fmt.Printf("  🎯 Key '%s' -> Partition %d, Offset %d\n", key, result.Partition, result.Offset)
	}
	
	// Show partition distribution
	fmt.Println("\n📊 Partition Distribution Analysis:")
	partitionCounts := make(map[int32]int)
	
	for i := int32(0); i < 4; i++ {
		messages, err := consumer.Consume(ctx, topic, i, 0, 100)
		if err != nil {
			continue
		}
		partitionCounts[i] = len(messages)
		fmt.Printf("  📈 Partition %d: %d messages\n", i, len(messages))
	}
	
	return nil
}

func demoConsumerGroups(ctx context.Context, c *client.Client) error {
	// This would demonstrate consumer group coordination in a real implementation
	// For now, we'll simulate multiple consumers reading from different partitions
	
	consumer1, err := c.NewConsumer()
	if err != nil {
		return fmt.Errorf("failed to create consumer 1: %v", err)
	}
	defer consumer1.Close()

	consumer2, err := c.NewConsumer()
	if err != nil {
		return fmt.Errorf("failed to create consumer 2: %v", err)
	}
	defer consumer2.Close()

	topic := "phase2-group-topic"
	
	// Simulate consumer group by having each consumer read from different partitions
	fmt.Println("👥 Simulating consumer group with partition assignment:")
	
	fmt.Println("  🔄 Consumer 1 reading from partitions 0-1")
	for partition := int32(0); partition < 2; partition++ {
		messages, err := consumer1.Consume(ctx, topic, partition, 0, 10)
		if err != nil {
			continue
		}
		fmt.Printf("    📥 Consumer 1 got %d messages from partition %d\n", len(messages), partition)
	}
	
	fmt.Println("  🔄 Consumer 2 reading from partitions 2-3")
	for partition := int32(2); partition < 4; partition++ {
		messages, err := consumer2.Consume(ctx, topic, partition, 0, 10)
		if err != nil {
			continue
		}
		fmt.Printf("    📥 Consumer 2 got %d messages from partition %d\n", len(messages), partition)
	}
	
	// Show metrics for both consumers
	metrics1 := consumer1.GetMetrics()
	metrics2 := consumer2.GetMetrics()
	
	fmt.Printf("📊 Consumer 1 metrics: %d messages received, %d errors\n", 
		metrics1.MessagesReceived, metrics1.Errors)
	fmt.Printf("📊 Consumer 2 metrics: %d messages received, %d errors\n", 
		metrics2.MessagesReceived, metrics2.Errors)
	
	return nil
}