package coordination

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerGroupCoordinator manages consumer group membership and partition assignment
type ConsumerGroupCoordinator struct {
	groups          map[string]*ConsumerGroup
	consumers       map[string]*ConsumerMember
	partitionTopics map[string][]int32 // topic -> partitions
	mu              sync.RWMutex
	
	// Rebalancing
	rebalanceQueue chan *ConsumerGroup
	rebalancingMu  sync.Mutex
	
	// Background processing
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ConsumerGroup represents a group of consumers
type ConsumerGroup struct {
	ID                string                      `json:"id"`
	Members           map[string]*ConsumerMember  `json:"members"`
	PartitionAssignment map[string][]int32        `json:"partition_assignment"` // consumer_id -> partitions
	Topics            []string                    `json:"topics"`
	CreatedAt         time.Time                   `json:"created_at"`
	LastRebalance     time.Time                   `json:"last_rebalance"`
	State             GroupState                  `json:"state"`
	mu                sync.RWMutex
}

// ConsumerMember represents a consumer in a group
type ConsumerMember struct {
	ID              string            `json:"id"`
	GroupID         string            `json:"group_id"`
	Topics          []string          `json:"topics"`
	AssignedPartitions []int32        `json:"assigned_partitions"`
	LastHeartbeat   time.Time         `json:"last_heartbeat"`
	SessionTimeout  time.Duration     `json:"session_timeout"`
	State           ConsumerState     `json:"state"`
	Metadata        map[string]string `json:"metadata"`
}

// GroupState represents the state of a consumer group
type GroupState int

const (
	GroupStateStable GroupState = iota
	GroupStateRebalancing
	GroupStateEmpty
	GroupStateDead
)

func (gs GroupState) String() string {
	switch gs {
	case GroupStateStable:
		return "Stable"
	case GroupStateRebalancing:
		return "Rebalancing"
	case GroupStateEmpty:
		return "Empty"
	case GroupStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// ConsumerState represents the state of a consumer
type ConsumerState int

const (
	ConsumerStateActive ConsumerState = iota
	ConsumerStateInactive
	ConsumerStateRebalancing
	ConsumerStateLeaving
)

func (cs ConsumerState) String() string {
	switch cs {
	case ConsumerStateActive:
		return "Active"
	case ConsumerStateInactive:
		return "Inactive"
	case ConsumerStateRebalancing:
		return "Rebalancing"
	case ConsumerStateLeaving:
		return "Leaving"
	default:
		return "Unknown"
	}
}

// NewConsumerGroupCoordinator creates a new consumer group coordinator
func NewConsumerGroupCoordinator() *ConsumerGroupCoordinator {
	cgc := &ConsumerGroupCoordinator{
		groups:          make(map[string]*ConsumerGroup),
		consumers:       make(map[string]*ConsumerMember),
		partitionTopics: make(map[string][]int32),
		rebalanceQueue:  make(chan *ConsumerGroup, 100),
		stopCh:          make(chan struct{}),
	}

	// Start background processes
	cgc.wg.Add(2)
	go cgc.heartbeatMonitor()
	go cgc.rebalanceProcessor()

	return cgc
}

// JoinGroup adds a consumer to a group
func (cgc *ConsumerGroupCoordinator) JoinGroup(consumerID, groupID string, topics []string, sessionTimeout time.Duration) error {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	// Create group if it doesn't exist
	group, exists := cgc.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			ID:                  groupID,
			Members:             make(map[string]*ConsumerMember),
			PartitionAssignment: make(map[string][]int32),
			Topics:              topics,
			CreatedAt:           time.Now(),
			State:               GroupStateEmpty,
		}
		cgc.groups[groupID] = group
	}

	// Validate topics match group topics
	if !cgc.topicsMatch(group.Topics, topics) {
		return fmt.Errorf("consumer topics %v don't match group topics %v", topics, group.Topics)
	}

	// Create or update consumer member
	member := &ConsumerMember{
		ID:              consumerID,
		GroupID:         groupID,
		Topics:          topics,
		LastHeartbeat:   time.Now(),
		SessionTimeout:  sessionTimeout,
		State:           ConsumerStateActive,
		Metadata:        make(map[string]string),
	}

	group.mu.Lock()
	group.Members[consumerID] = member
	group.State = GroupStateRebalancing
	group.mu.Unlock()

	cgc.consumers[consumerID] = member

	// Trigger rebalance
	select {
	case cgc.rebalanceQueue <- group:
	default:
		// Queue full, skip (rebalance will happen in next cycle)
	}

	return nil
}

// LeaveGroup removes a consumer from a group
func (cgc *ConsumerGroupCoordinator) LeaveGroup(consumerID string) error {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	member, exists := cgc.consumers[consumerID]
	if !exists {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	group, exists := cgc.groups[member.GroupID]
	if !exists {
		return fmt.Errorf("group %s not found", member.GroupID)
	}

	group.mu.Lock()
	member.State = ConsumerStateLeaving
	delete(group.Members, consumerID)
	delete(group.PartitionAssignment, consumerID)
	
	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
	} else {
		group.State = GroupStateRebalancing
	}
	group.mu.Unlock()

	delete(cgc.consumers, consumerID)

	// Trigger rebalance
	select {
	case cgc.rebalanceQueue <- group:
	default:
	}

	return nil
}

// Heartbeat updates the consumer's heartbeat
func (cgc *ConsumerGroupCoordinator) Heartbeat(consumerID string) error {
	cgc.mu.RLock()
	member, exists := cgc.consumers[consumerID]
	cgc.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	member.LastHeartbeat = time.Now()
	return nil
}

// GetGroupAssignment returns the partition assignment for a consumer
func (cgc *ConsumerGroupCoordinator) GetGroupAssignment(consumerID string) ([]int32, error) {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()

	member, exists := cgc.consumers[consumerID]
	if !exists {
		return nil, fmt.Errorf("consumer %s not found", consumerID)
	}

	group, exists := cgc.groups[member.GroupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", member.GroupID)
	}

	group.mu.RLock()
	assignment := group.PartitionAssignment[consumerID]
	group.mu.RUnlock()

	return assignment, nil
}

// SetTopicPartitions sets the available partitions for a topic
func (cgc *ConsumerGroupCoordinator) SetTopicPartitions(topic string, partitions []int32) {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()
	cgc.partitionTopics[topic] = partitions
}

// topicsMatch checks if two topic lists are equivalent
func (cgc *ConsumerGroupCoordinator) topicsMatch(topics1, topics2 []string) bool {
	if len(topics1) != len(topics2) {
		return false
	}

	topicMap := make(map[string]bool)
	for _, topic := range topics1 {
		topicMap[topic] = true
	}

	for _, topic := range topics2 {
		if !topicMap[topic] {
			return false
		}
	}

	return true
}

// rebalanceGroup assigns partitions to consumers in a group
func (cgc *ConsumerGroupCoordinator) rebalanceGroup(group *ConsumerGroup) {
	group.mu.Lock()
	defer group.mu.Unlock()

	// Clear existing assignments
	group.PartitionAssignment = make(map[string][]int32)

	// Get active members
	activeMembers := make([]*ConsumerMember, 0)
	for _, member := range group.Members {
		if member.State == ConsumerStateActive {
			activeMembers = append(activeMembers, member)
		}
	}

	if len(activeMembers) == 0 {
		group.State = GroupStateEmpty
		return
	}

	// Collect all partitions for topics
	allPartitions := make([]TopicPartition, 0)
	for _, topic := range group.Topics {
		if partitions, exists := cgc.partitionTopics[topic]; exists {
			for _, partition := range partitions {
				allPartitions = append(allPartitions, TopicPartition{
					Topic:     topic,
					Partition: partition,
				})
			}
		}
	}

	// Simple round-robin assignment
	memberIndex := 0
	for _, tp := range allPartitions {
		member := activeMembers[memberIndex]
		
		// For simplicity, we'll assign all partitions of a topic to consumers
		// In a real implementation, you might want more sophisticated strategies
		if assignment, exists := group.PartitionAssignment[member.ID]; exists {
			group.PartitionAssignment[member.ID] = append(assignment, tp.Partition)
		} else {
			group.PartitionAssignment[member.ID] = []int32{tp.Partition}
		}
		
		member.AssignedPartitions = group.PartitionAssignment[member.ID]
		memberIndex = (memberIndex + 1) % len(activeMembers)
	}

	group.State = GroupStateStable
	group.LastRebalance = time.Now()
}

// heartbeatMonitor monitors consumer heartbeats and removes inactive consumers
func (cgc *ConsumerGroupCoordinator) heartbeatMonitor() {
	defer cgc.wg.Done()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cgc.checkInactiveConsumers()
		case <-cgc.stopCh:
			return
		}
	}
}

// checkInactiveConsumers removes consumers that haven't sent heartbeats
func (cgc *ConsumerGroupCoordinator) checkInactiveConsumers() {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	now := time.Now()
	inactiveConsumers := make([]string, 0)

	for consumerID, member := range cgc.consumers {
		if now.Sub(member.LastHeartbeat) > member.SessionTimeout*2 {
			inactiveConsumers = append(inactiveConsumers, consumerID)
		}
	}

	// Remove inactive consumers
	for _, consumerID := range inactiveConsumers {
		member := cgc.consumers[consumerID]
		if group, exists := cgc.groups[member.GroupID]; exists {
			group.mu.Lock()
			delete(group.Members, consumerID)
			delete(group.PartitionAssignment, consumerID)
			
			if len(group.Members) == 0 {
				group.State = GroupStateEmpty
			} else {
				group.State = GroupStateRebalancing
				// Trigger rebalance
				select {
				case cgc.rebalanceQueue <- group:
				default:
				}
			}
			group.mu.Unlock()
		}
		delete(cgc.consumers, consumerID)
	}
}

// rebalanceProcessor handles rebalancing requests
func (cgc *ConsumerGroupCoordinator) rebalanceProcessor() {
	defer cgc.wg.Done()

	for {
		select {
		case group := <-cgc.rebalanceQueue:
			cgc.rebalancingMu.Lock()
			cgc.rebalanceGroup(group)
			cgc.rebalancingMu.Unlock()
		case <-cgc.stopCh:
			return
		}
	}
}

// GetGroupInfo returns information about a consumer group
func (cgc *ConsumerGroupCoordinator) GetGroupInfo(groupID string) (*GroupInfo, error) {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()

	group, exists := cgc.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	members := make([]*ConsumerMember, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, &ConsumerMember{
			ID:                 member.ID,
			GroupID:            member.GroupID,
			Topics:             member.Topics,
			AssignedPartitions: member.AssignedPartitions,
			LastHeartbeat:      member.LastHeartbeat,
			SessionTimeout:     member.SessionTimeout,
			State:              member.State,
		})
	}

	return &GroupInfo{
		ID:              group.ID,
		Members:         members,
		Topics:          group.Topics,
		CreatedAt:       group.CreatedAt,
		LastRebalance:   group.LastRebalance,
		State:           group.State,
		MemberCount:     len(group.Members),
		PartitionCount:  cgc.getGroupPartitionCount(group),
	}, nil
}

func (cgc *ConsumerGroupCoordinator) getGroupPartitionCount(group *ConsumerGroup) int {
	totalPartitions := 0
	for _, topic := range group.Topics {
		if partitions, exists := cgc.partitionTopics[topic]; exists {
			totalPartitions += len(partitions)
		}
	}
	return totalPartitions
}

// Stop gracefully shuts down the coordinator
func (cgc *ConsumerGroupCoordinator) Stop() {
	close(cgc.stopCh)
	cgc.wg.Wait()
}

// TopicPartition represents a topic-partition pair
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// GroupInfo contains information about a consumer group
type GroupInfo struct {
	ID             string             `json:"id"`
	Members        []*ConsumerMember  `json:"members"`
	Topics         []string           `json:"topics"`
	CreatedAt      time.Time          `json:"created_at"`
	LastRebalance  time.Time          `json:"last_rebalance"`
	State          GroupState         `json:"state"`
	MemberCount    int                `json:"member_count"`
	PartitionCount int                `json:"partition_count"`
}