package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Anujtr/streamflow-engine/pkg/client"
)

func main() {
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
	
	fmt.Printf("Server Status: %s (Version: %s)\n", health.Status, health.Version)
	fmt.Printf("Server Metrics: %+v\n\n", health.Metrics)

	// Create producer
	producer, err := c.NewProducer()
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create consumer
	consumer, err := c.NewConsumer()
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	topic := "example-topic"

	// Produce some messages
	fmt.Println("Producing messages...")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("Hello StreamFlow! Message %d", i)
		
		result, err := producer.SendSingle(ctx, topic, key, []byte(value))
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue
		}
		
		if result.Error != "" {
			log.Printf("Server error for message %d: %s", i, result.Error)
			continue
		}
		
		fmt.Printf("Sent message %d to partition %d at offset %d\n", i, result.Partition, result.Offset)
	}

	// Give some time for messages to be stored
	time.Sleep(1 * time.Second)

	// Consume messages from each partition
	fmt.Println("\nConsuming messages...")
	for partition := int32(0); partition < 4; partition++ {
		fmt.Printf("Reading from partition %d:\n", partition)
		
		messages, err := consumer.Consume(ctx, topic, partition, 0, 10)
		if err != nil {
			log.Printf("Failed to consume from partition %d: %v", partition, err)
			continue
		}
		
		for _, msg := range messages {
			fmt.Printf("  Partition %d, Offset %d: %s = %s (at %v)\n", 
				msg.Partition, msg.Offset, msg.Key, string(msg.Value), msg.Timestamp.Format(time.RFC3339))
		}
	}

	// Show metrics
	fmt.Println("\nMetrics:")
	producerMetrics := producer.GetMetrics()
	fmt.Printf("Producer - Messages Sent: %d, Errors: %d\n", 
		producerMetrics.MessagesSent, producerMetrics.Errors)
	
	consumerMetrics := consumer.GetMetrics()
	fmt.Printf("Consumer - Messages Received: %d, Errors: %d\n", 
		consumerMetrics.MessagesReceived, consumerMetrics.Errors)

	// Final health check
	fmt.Println("\nFinal server health:")
	health, err = c.Health(ctx)
	if err != nil {
		log.Printf("Final health check failed: %v", err)
	} else {
		fmt.Printf("Messages Produced: %s, Messages Consumed: %s\n", 
			health.Metrics["messages_produced"], health.Metrics["messages_consumed"])
	}
}