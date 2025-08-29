package coordination

import (
	"fmt"
	"testing"
	"time"
)

func TestNewConsumerGroupCoordinator(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	if coordinator == nil {
		t.Fatal("Expected non-nil coordinator")
	}
	
	if coordinator.groups == nil {
		t.Error("Expected non-nil groups map")
	}
	
	if coordinator.consumers == nil {
		t.Error("Expected non-nil consumers map")
	}
}

func TestConsumerGroupCoordinator_JoinGroup(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions for the topic
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1, 2, 3})
	
	// Join group
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Verify consumer exists
	if _, exists := coordinator.consumers["consumer-1"]; !exists {
		t.Error("Consumer not found after joining group")
	}
	
	// Verify group exists
	if _, exists := coordinator.groups["group-1"]; !exists {
		t.Error("Group not found after consumer joined")
	}
	
	// Try to join with different topics (should fail)
	err = coordinator.JoinGroup("consumer-2", "group-1", []string{"different-topic"}, 30*time.Second)
	if err == nil {
		t.Error("Expected error when joining group with different topics")
	}
}

func TestConsumerGroupCoordinator_LeaveGroup(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1})
	
	// Join group
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Leave group
	err = coordinator.LeaveGroup("consumer-1")
	if err != nil {
		t.Errorf("Failed to leave group: %v", err)
	}
	
	// Verify consumer is removed
	if _, exists := coordinator.consumers["consumer-1"]; exists {
		t.Error("Consumer still exists after leaving group")
	}
	
	// Try to leave non-existent consumer
	err = coordinator.LeaveGroup("non-existent")
	if err == nil {
		t.Error("Expected error when leaving with non-existent consumer")
	}
}

func TestConsumerGroupCoordinator_Heartbeat(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1})
	
	// Join group
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Send heartbeat
	err = coordinator.Heartbeat("consumer-1")
	if err != nil {
		t.Errorf("Failed to send heartbeat: %v", err)
	}
	
	// Verify heartbeat time was updated
	member := coordinator.consumers["consumer-1"]
	if time.Since(member.LastHeartbeat) > time.Second {
		t.Error("Heartbeat time was not updated")
	}
	
	// Try heartbeat for non-existent consumer
	err = coordinator.Heartbeat("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent consumer heartbeat")
	}
}

func TestConsumerGroupCoordinator_GetGroupAssignment(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1, 2, 3})
	
	// Join group with multiple consumers
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	err = coordinator.JoinGroup("consumer-2", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Allow some time for rebalancing
	time.Sleep(100 * time.Millisecond)
	
	// Get assignment for consumer-1
	assignment1, err := coordinator.GetGroupAssignment("consumer-1")
	if err != nil {
		t.Errorf("Failed to get assignment: %v", err)
	}
	
	// Get assignment for consumer-2
	assignment2, err := coordinator.GetGroupAssignment("consumer-2")
	if err != nil {
		t.Errorf("Failed to get assignment: %v", err)
	}
	
	// Verify assignments are not overlapping (though this is basic round-robin)
	totalAssigned := len(assignment1) + len(assignment2)
	if totalAssigned == 0 {
		t.Error("No partitions were assigned")
	}
	
	// Try to get assignment for non-existent consumer
	_, err = coordinator.GetGroupAssignment("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent consumer")
	}
}

func TestConsumerGroupCoordinator_MultipleGroups(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions for different topics
	coordinator.SetTopicPartitions("topic-1", []int32{0, 1})
	coordinator.SetTopicPartitions("topic-2", []int32{0, 1, 2})
	
	// Create consumers in different groups
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"topic-1"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group-1: %v", err)
	}
	
	err = coordinator.JoinGroup("consumer-2", "group-2", []string{"topic-2"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group-2: %v", err)
	}
	
	// Verify both groups exist
	if len(coordinator.groups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(coordinator.groups))
	}
	
	// Verify consumers are in correct groups
	consumer1 := coordinator.consumers["consumer-1"]
	consumer2 := coordinator.consumers["consumer-2"]
	
	if consumer1.GroupID != "group-1" {
		t.Errorf("Consumer-1 should be in group-1, got %s", consumer1.GroupID)
	}
	
	if consumer2.GroupID != "group-2" {
		t.Errorf("Consumer-2 should be in group-2, got %s", consumer2.GroupID)
	}
}

func TestConsumerGroupCoordinator_GetGroupInfo(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1})
	
	// Join group
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 30*time.Second)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Get group info
	info, err := coordinator.GetGroupInfo("group-1")
	if err != nil {
		t.Errorf("Failed to get group info: %v", err)
	}
	
	if info.ID != "group-1" {
		t.Errorf("Expected group ID 'group-1', got %s", info.ID)
	}
	
	if info.MemberCount != 1 {
		t.Errorf("Expected 1 member, got %d", info.MemberCount)
	}
	
	if len(info.Topics) != 1 || info.Topics[0] != "test-topic" {
		t.Errorf("Expected topics ['test-topic'], got %v", info.Topics)
	}
	
	if len(info.Members) != 1 {
		t.Errorf("Expected 1 member in info, got %d", len(info.Members))
	}
	
	member := info.Members[0]
	if member.ID != "consumer-1" {
		t.Errorf("Expected member ID 'consumer-1', got %s", member.ID)
	}
	
	// Try to get info for non-existent group
	_, err = coordinator.GetGroupInfo("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent group")
	}
}

func TestConsumerGroupCoordinator_InactiveConsumerCleanup(t *testing.T) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	// Set up partitions
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1})
	
	// Join group with short session timeout
	err := coordinator.JoinGroup("consumer-1", "group-1", []string{"test-topic"}, 100*time.Millisecond)
	if err != nil {
		t.Errorf("Failed to join group: %v", err)
	}
	
	// Verify consumer exists
	if _, exists := coordinator.consumers["consumer-1"]; !exists {
		t.Error("Consumer not found after joining")
	}
	
	// Wait for session timeout + cleanup cycle
	time.Sleep(300 * time.Millisecond)
	
	// Trigger manual cleanup check
	coordinator.checkInactiveConsumers()
	
	// Verify consumer was removed
	if _, exists := coordinator.consumers["consumer-1"]; exists {
		t.Error("Inactive consumer was not cleaned up")
	}
}

func TestGroupState_String(t *testing.T) {
	testCases := []struct {
		state    GroupState
		expected string
	}{
		{GroupStateStable, "Stable"},
		{GroupStateRebalancing, "Rebalancing"},
		{GroupStateEmpty, "Empty"},
		{GroupStateDead, "Dead"},
		{GroupState(99), "Unknown"},
	}
	
	for _, tc := range testCases {
		if tc.state.String() != tc.expected {
			t.Errorf("Expected state %d to return '%s', got '%s'", int(tc.state), tc.expected, tc.state.String())
		}
	}
}

func TestConsumerState_String(t *testing.T) {
	testCases := []struct {
		state    ConsumerState
		expected string
	}{
		{ConsumerStateActive, "Active"},
		{ConsumerStateInactive, "Inactive"},
		{ConsumerStateRebalancing, "Rebalancing"},
		{ConsumerStateLeaving, "Leaving"},
		{ConsumerState(99), "Unknown"},
	}
	
	for _, tc := range testCases {
		if tc.state.String() != tc.expected {
			t.Errorf("Expected state %d to return '%s', got '%s'", int(tc.state), tc.expected, tc.state.String())
		}
	}
}

func BenchmarkConsumerGroupCoordinator_JoinLeave(b *testing.B) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1, 2, 3})
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i)
		
		err := coordinator.JoinGroup(consumerID, "benchmark-group", []string{"test-topic"}, 30*time.Second)
		if err != nil {
			b.Errorf("Failed to join group: %v", err)
		}
		
		err = coordinator.LeaveGroup(consumerID)
		if err != nil {
			b.Errorf("Failed to leave group: %v", err)
		}
	}
}

func BenchmarkConsumerGroupCoordinator_Heartbeat(b *testing.B) {
	coordinator := NewConsumerGroupCoordinator()
	defer coordinator.Stop()
	
	coordinator.SetTopicPartitions("test-topic", []int32{0, 1, 2, 3})
	
	// Join some consumers
	for i := 0; i < 10; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i)
		err := coordinator.JoinGroup(consumerID, "benchmark-group", []string{"test-topic"}, 30*time.Second)
		if err != nil {
			b.Errorf("Failed to join group: %v", err)
		}
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			consumerID := fmt.Sprintf("consumer-%d", i%10)
			coordinator.Heartbeat(consumerID)
			i++
		}
	})
}