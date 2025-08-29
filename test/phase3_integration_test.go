package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/coordination"
	"github.com/Anujtr/streamflow-engine/internal/health"
	"github.com/Anujtr/streamflow-engine/internal/partitioning"
	"github.com/Anujtr/streamflow-engine/internal/storage"
)

func TestPhase3_PersistentStorage_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create persistent storage
	storageConfig := storage.StorageConfig{
		DataDir:         tmpDir,
		PersistenceMode: true,
	}
	
	store, err := storage.NewPersistentStorage(storageConfig)
	if err != nil {
		t.Fatalf("Failed to create persistent storage: %v", err)
	}
	defer store.Close()

	// Create partition manager
	partitionManager := partitioning.NewPartitionManager(store)

	// Create a topic with partitions
	topicName := "integration-test-topic"
	partitionCount := int32(4)

	err = partitionManager.CreateTopicPartitions(topicName, partitionCount)
	if err != nil {
		t.Fatalf("Failed to create topic partitions: %v", err)
	}

	// Test message production and consumption
	numMessages := 100
	
	// Produce messages
	for i := 0; i < numMessages; i++ {
		key := fmt.Sprintf("key-%d", i)
		message := &storage.Message{
			Key:   key,
			Value: []byte(fmt.Sprintf("persistent-message-%d", i)),
		}

		partition, offset, err := store.Produce(topicName, message)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}

		if partition < 0 || partition >= partitionCount {
			t.Errorf("Invalid partition %d for message %d", partition, i)
		}

		if offset < 0 {
			t.Errorf("Invalid offset %d for message %d", offset, i)
		}
	}

	// Consume messages from all partitions
	totalConsumed := 0
	for partition := int32(0); partition < partitionCount; partition++ {
		messages, hasMore, err := store.Consume(topicName, partition, 0, 100)
		if err != nil {
			t.Fatalf("Failed to consume from partition %d: %v", partition, err)
		}

		totalConsumed += len(messages)
		
		// Verify message content
		for _, msg := range messages {
			if msg.Key == "" || len(msg.Value) == 0 {
				t.Errorf("Invalid message in partition %d: key=%s, value=%s", partition, msg.Key, string(msg.Value))
			}
		}

		// For testing, we don't expect more messages
		if hasMore && len(messages) == 100 {
			t.Logf("Partition %d has more messages (expected for large datasets)", partition)
		}
	}

	if totalConsumed != numMessages {
		t.Errorf("Expected to consume %d messages, got %d", numMessages, totalConsumed)
	}

	// Test partition metrics
	metrics, err := partitionManager.GetAllPartitionMetrics(topicName)
	if err != nil {
		t.Fatalf("Failed to get partition metrics: %v", err)
	}

	if len(metrics) != int(partitionCount) {
		t.Errorf("Expected %d partition metrics, got %d", partitionCount, len(metrics))
	}
}

func TestPhase3_OffsetManagement_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create persistent storage
	storageConfig := storage.StorageConfig{
		DataDir:         tmpDir,
		PersistenceMode: true,
	}
	
	store, err := storage.NewPersistentStorage(storageConfig)
	if err != nil {
		t.Fatalf("Failed to create persistent storage: %v", err)
	}
	defer store.Close()

	// Create topic and produce messages
	topicName := "offset-test-topic"
	err = store.CreateTopic(topicName, 2)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		message := &storage.Message{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("offset-test-message-%d", i)),
		}
		
		_, _, err := store.Produce(topicName, message)
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Test offset management
	offsetStore := store.GetOffsetStore()
	if offsetStore == nil {
		t.Fatal("Expected offset store to be available with persistent storage")
	}

	consumerGroup := "test-consumer-group"
	partition := int32(0)
	commitOffset := int64(5)

	// Commit an offset
	err = offsetStore.CommitOffset(consumerGroup, topicName, partition, commitOffset)
	if err != nil {
		t.Fatalf("Failed to commit offset: %v", err)
	}

	// Retrieve the offset
	retrievedOffset, err := offsetStore.GetOffset(consumerGroup, topicName, partition)
	if err != nil {
		t.Fatalf("Failed to get offset: %v", err)
	}

	if retrievedOffset != commitOffset {
		t.Errorf("Expected offset %d, got %d", commitOffset, retrievedOffset)
	}

	// Test offset persistence across storage restart
	store.Close()

	// Reopen storage
	store, err = storage.NewPersistentStorage(storageConfig)
	if err != nil {
		t.Fatalf("Failed to reopen persistent storage: %v", err)
	}
	defer store.Close()

	offsetStore = store.GetOffsetStore()
	retrievedOffset, err = offsetStore.GetOffset(consumerGroup, topicName, partition)
	if err != nil {
		t.Fatalf("Failed to get offset after restart: %v", err)
	}

	if retrievedOffset != commitOffset {
		t.Errorf("Expected persisted offset %d, got %d", commitOffset, retrievedOffset)
	}
}

func TestPhase3_HealthMonitoring_Integration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create health monitor
	healthMonitor := health.NewHealthMonitor(health.HealthMonitorConfig{
		NodeID:        "test-node",
		CheckInterval: 1 * time.Second,
		CheckTimeout:  500 * time.Millisecond,
	})

	// Create storage and register it
	storageConfig := storage.StorageConfig{
		DataDir:         tmpDir,
		PersistenceMode: true,
	}
	
	store, err := storage.NewPersistentStorage(storageConfig)
	if err != nil {
		t.Fatalf("Failed to create persistent storage: %v", err)
	}
	defer store.Close()

	healthMonitor.RegisterStorage(store)
	healthMonitor.Start()
	defer healthMonitor.Stop()

	// Wait for initial health check
	time.Sleep(100 * time.Millisecond)

	// Get health status
	health := healthMonitor.GetHealth()
	if health == nil {
		t.Fatal("Expected health status to be available")
	}

	if health.NodeID != "test-node" {
		t.Errorf("Expected node ID 'test-node', got '%s'", health.NodeID)
	}

	if health.Status != "healthy" {
		t.Errorf("Expected health status 'healthy', got '%s'", health.Status)
	}

	if len(health.Components) == 0 {
		t.Error("Expected at least one component to be monitored")
	}

	// Check storage component
	if storageHealth, exists := health.Components["storage"]; exists {
		if storageHealth.Status != "healthy" {
			t.Errorf("Expected storage to be healthy, got '%s'", storageHealth.Status)
		}
		if storageHealth.Details["persistence"] != "enabled" {
			t.Errorf("Expected persistence to be enabled, got '%s'", storageHealth.Details["persistence"])
		}
	} else {
		t.Error("Expected storage component to be monitored")
	}

	if health.Uptime <= 0 {
		t.Error("Expected positive uptime")
	}
}

func TestPhase3_PartitionManager_WithoutLeaderElection(t *testing.T) {
	tmpDir := t.TempDir()

	// Create storage
	storageConfig := storage.StorageConfig{
		DataDir:         tmpDir,
		PersistenceMode: true,
	}
	
	store, err := storage.NewPersistentStorage(storageConfig)
	if err != nil {
		t.Fatalf("Failed to create persistent storage: %v", err)
	}
	defer store.Close()

	// Create partition manager without leader election (single-node mode)
	partitionManager := partitioning.NewPartitionManager(store)

	topicName := "partition-test-topic"
	initialPartitions := int32(2)

	// Create topic with partitions
	err = partitionManager.CreateTopicPartitions(topicName, initialPartitions)
	if err != nil {
		t.Fatalf("Failed to create topic partitions: %v", err)
	}

	// Test partition assignment
	testKeys := []string{"user-1", "user-2", "order-123", "session-abc", "event-xyz"}
	partitionCounts := make(map[int32]int)

	for _, key := range testKeys {
		partition, err := partitionManager.GetPartition(topicName, key)
		if err != nil {
			t.Fatalf("Failed to get partition for key %s: %v", key, err)
		}

		if partition < 0 || partition >= initialPartitions {
			t.Errorf("Invalid partition %d for key %s (expected 0-%d)", partition, key, initialPartitions-1)
		}

		partitionCounts[partition]++
	}

	// Verify some distribution (not necessarily even)
	if len(partitionCounts) == 0 {
		t.Error("Expected some partition assignments")
	}

	// Test partition scaling
	newPartitionCount := int32(4)
	err = partitionManager.ScalePartitions(topicName, newPartitionCount)
	if err != nil {
		t.Fatalf("Failed to scale partitions: %v", err)
	}

	// Verify partition count after scaling
	topicPartitions, err := partitionManager.GetTopicPartitions(topicName)
	if err != nil {
		t.Fatalf("Failed to get topic partitions after scaling: %v", err)
	}

	if topicPartitions.PartitionCount != newPartitionCount {
		t.Errorf("Expected %d partitions after scaling, got %d", newPartitionCount, topicPartitions.PartitionCount)
	}

	// Test partition leadership (should always be true in single-node mode)
	for i := int32(0); i < newPartitionCount; i++ {
		isLeader := partitionManager.IsPartitionLeader(topicName, i)
		if !isLeader {
			t.Errorf("Expected to be leader for partition %d in single-node mode", i)
		}
	}

	// Get manager stats
	stats := partitionManager.GetManagerStats()
	if stats.TopicCount != 1 {
		t.Errorf("Expected 1 topic, got %d", stats.TopicCount)
	}
	if stats.TotalPartitions != int(newPartitionCount) {
		t.Errorf("Expected %d total partitions, got %d", newPartitionCount, stats.TotalPartitions)
	}
}

func TestPhase3_ConsumerGroupCoordinator_Integration(t *testing.T) {
	// Create consumer group coordinator
	coordinator := coordination.NewConsumerGroupCoordinator()
	defer coordinator.Stop()

	groupID := "integration-test-group"
	consumerID1 := "consumer-1"
	consumerID2 := "consumer-2"
	topic := "test-topic"
	partitions := []int32{0, 1, 2, 3}

	// Set topic partitions for the coordinator
	coordinator.SetTopicPartitions(topic, partitions)

	// Test consumer joining
	err := coordinator.JoinGroup(consumerID1, groupID, []string{topic}, 30*time.Second)
	if err != nil {
		t.Fatalf("Failed to join consumer 1: %v", err)
	}

	err = coordinator.JoinGroup(consumerID2, groupID, []string{topic}, 30*time.Second)
	if err != nil {
		t.Fatalf("Failed to join consumer 2: %v", err)
	}

	// Wait for rebalancing to complete
	time.Sleep(100 * time.Millisecond)

	// Test partition assignment
	assignment1, err := coordinator.GetGroupAssignment(consumerID1)
	if err != nil {
		t.Fatalf("Failed to get partition assignment for consumer 1: %v", err)
	}

	assignment2, err := coordinator.GetGroupAssignment(consumerID2)
	if err != nil {
		t.Fatalf("Failed to get partition assignment for consumer 2: %v", err)
	}

	// Verify both consumers have assignments
	if len(assignment1) == 0 {
		t.Error("Expected consumer 1 to have partition assignments")
	}
	if len(assignment2) == 0 {
		t.Error("Expected consumer 2 to have partition assignments")
	}

	// Verify no partition overlap
	assignedPartitions := make(map[int32]bool)
	for _, partition := range assignment1 {
		assignedPartitions[partition] = true
	}
	for _, partition := range assignment2 {
		if assignedPartitions[partition] {
			t.Errorf("Partition %d assigned to multiple consumers", partition)
		}
	}

	// Test heartbeat
	err = coordinator.Heartbeat(consumerID1)
	if err != nil {
		t.Fatalf("Failed to send heartbeat for consumer 1: %v", err)
	}

	// Test group info
	groupInfo, err := coordinator.GetGroupInfo(groupID)
	if err != nil {
		t.Fatalf("Failed to get group info: %v", err)
	}

	if len(groupInfo.Members) != 2 {
		t.Errorf("Expected 2 group members, got %d", len(groupInfo.Members))
	}

	// Test consumer leaving
	err = coordinator.LeaveGroup(consumerID1)
	if err != nil {
		t.Fatalf("Failed to leave consumer group: %v", err)
	}

	// Wait for rebalancing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify group info updated
	groupInfo, err = coordinator.GetGroupInfo(groupID)
	if err != nil {
		t.Fatalf("Failed to get group info after leave: %v", err)
	}

	if len(groupInfo.Members) != 1 {
		t.Errorf("Expected 1 group member after leave, got %d", len(groupInfo.Members))
	}
}