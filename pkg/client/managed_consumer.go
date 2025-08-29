package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ManagedConsumer provides automatic offset management and at-least-once delivery
type ManagedConsumer struct {
	client        pb.MessageServiceClient
	conn          *grpc.ClientConn
	config        ManagedConsumerConfig
	
	// Offset management
	committedOffsets  map[string]map[int32]int64 // topic -> partition -> offset
	processingOffsets map[string]map[int32]int64 // topic -> partition -> processing offset
	offsetMu          sync.RWMutex
	
	// Auto-commit
	autoCommitTicker *time.Ticker
	stopAutoCommit   chan struct{}
	
	// Metrics
	messagesReceived int64
	messagesCommitted int64
	errors           int64
	mu               sync.RWMutex
}

// ManagedConsumerConfig holds configuration for the managed consumer
type ManagedConsumerConfig struct {
	Address                 string        `json:"address"`
	ConsumerGroup           string        `json:"consumer_group"`
	Timeout                 time.Duration `json:"timeout"`
	AutoCommitInterval      time.Duration `json:"auto_commit_interval"`
	EnableAutoCommit        bool          `json:"enable_auto_commit"`
	EnableAutoOffsetStore   bool          `json:"enable_auto_offset_store"`
	MaxMessages             int32         `json:"max_messages"`
}

// NewManagedConsumer creates a new managed consumer with offset tracking
func NewManagedConsumer(config ManagedConsumerConfig) (*ManagedConsumer, error) {
	if config.Address == "" {
		config.Address = "localhost:8080"
	}
	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "default"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = 100
	}

	// Connect to the server
	conn, err := grpc.Dial(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := pb.NewMessageServiceClient(conn)

	mc := &ManagedConsumer{
		client:            client,
		conn:              conn,
		config:            config,
		committedOffsets:  make(map[string]map[int32]int64),
		processingOffsets: make(map[string]map[int32]int64),
		stopAutoCommit:    make(chan struct{}),
	}

	// Start auto-commit if enabled
	if config.EnableAutoCommit {
		mc.startAutoCommit()
	}

	return mc, nil
}

// ConsumeFromCommittedOffset consumes messages starting from the last committed offset
func (mc *ManagedConsumer) ConsumeFromCommittedOffset(ctx context.Context, topic string, partition int32, maxMessages int32) ([]*ConsumeMessage, error) {
	// Get the last committed offset
	startOffset, err := mc.getCommittedOffset(ctx, topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get committed offset: %v", err)
	}

	// Consume messages from that offset
	messages, err := mc.consumeMessages(ctx, topic, partition, startOffset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Update processing offset
	if len(messages) > 0 {
		lastOffset := messages[len(messages)-1].Offset
		mc.setProcessingOffset(topic, partition, lastOffset+1)
	}

	return messages, nil
}

// ConsumeFromOffset consumes messages from a specific offset
func (mc *ManagedConsumer) ConsumeFromOffset(ctx context.Context, topic string, partition int32, offset int64, maxMessages int32) ([]*ConsumeMessage, error) {
	messages, err := mc.consumeMessages(ctx, topic, partition, offset, maxMessages)
	if err != nil {
		return nil, err
	}

	// Update processing offset
	if len(messages) > 0 {
		lastOffset := messages[len(messages)-1].Offset
		mc.setProcessingOffset(topic, partition, lastOffset+1)
	}

	return messages, nil
}

// CommitOffset commits the offset for a topic and partition
func (mc *ManagedConsumer) CommitOffset(ctx context.Context, topic string, partition int32, offset int64) error {
	req := &pb.CommitOffsetRequest{
		ConsumerGroup: mc.config.ConsumerGroup,
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
	}

	_, err := mc.client.CommitOffset(ctx, req)
	if err != nil {
		mc.incrementErrors()
		return fmt.Errorf("failed to commit offset: %v", err)
	}

	// Update local committed offset tracking
	mc.setCommittedOffset(topic, partition, offset)
	mc.incrementCommitted()

	return nil
}

// CommitProcessingOffsets commits all current processing offsets
func (mc *ManagedConsumer) CommitProcessingOffsets(ctx context.Context) error {
	mc.offsetMu.RLock()
	defer mc.offsetMu.RUnlock()

	for topic, partitions := range mc.processingOffsets {
		for partition, offset := range partitions {
			if err := mc.CommitOffset(ctx, topic, partition, offset); err != nil {
				log.Printf("Failed to commit offset for %s partition %d: %v", topic, partition, err)
				return err
			}
		}
	}

	return nil
}

// GetCommittedOffset returns the last committed offset for a topic and partition
func (mc *ManagedConsumer) GetCommittedOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	return mc.getCommittedOffset(ctx, topic, partition)
}

// Close closes the consumer and stops auto-commit
func (mc *ManagedConsumer) Close() error {
	if mc.config.EnableAutoCommit {
		mc.stopAutoCommit <- struct{}{}
		if mc.autoCommitTicker != nil {
			mc.autoCommitTicker.Stop()
		}
	}

	// Commit any pending offsets before closing
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := mc.CommitProcessingOffsets(ctx); err != nil {
		log.Printf("Failed to commit final offsets: %v", err)
	}

	return mc.conn.Close()
}

// GetMetrics returns consumer metrics
func (mc *ManagedConsumer) GetMetrics() ManagedConsumerMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	return ManagedConsumerMetrics{
		MessagesReceived:  mc.messagesReceived,
		MessagesCommitted: mc.messagesCommitted,
		Errors:           mc.errors,
	}
}

// Private methods

func (mc *ManagedConsumer) consumeMessages(ctx context.Context, topic string, partition int32, offset int64, maxMessages int32) ([]*ConsumeMessage, error) {
	req := &pb.ConsumeRequest{
		Topic:         topic,
		ConsumerGroup: mc.config.ConsumerGroup,
		Partition:     partition,
		Offset:        offset,
		MaxMessages:   maxMessages,
	}

	resp, err := mc.client.ConsumeBatch(ctx, req)
	if err != nil {
		mc.incrementErrors()
		return nil, fmt.Errorf("failed to consume messages: %v", err)
	}

	if resp.Error != "" {
		mc.incrementErrors()
		return nil, fmt.Errorf("server error: %s", resp.Error)
	}

	// Convert protobuf messages
	var messages []*ConsumeMessage
	for _, protoMsg := range resp.Messages {
		msg := &ConsumeMessage{
			Key:       protoMsg.Key,
			Value:     protoMsg.Value,
			Partition: protoMsg.Partition,
			Offset:    protoMsg.Offset,
			Timestamp: protoMsg.Timestamp.AsTime(),
		}
		messages = append(messages, msg)
	}

	mc.incrementReceived(int64(len(messages)))
	return messages, nil
}

func (mc *ManagedConsumer) getCommittedOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	// First check local cache
	mc.offsetMu.RLock()
	if topicOffsets, exists := mc.committedOffsets[topic]; exists {
		if offset, exists := topicOffsets[partition]; exists {
			mc.offsetMu.RUnlock()
			return offset, nil
		}
	}
	mc.offsetMu.RUnlock()

	// Fetch from server
	req := &pb.GetOffsetRequest{
		ConsumerGroup: mc.config.ConsumerGroup,
		Topic:         topic,
		Partition:     partition,
	}

	resp, err := mc.client.GetOffset(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to get offset: %v", err)
	}

	// Cache the result
	mc.setCommittedOffset(topic, partition, resp.Offset)
	return resp.Offset, nil
}

func (mc *ManagedConsumer) setCommittedOffset(topic string, partition int32, offset int64) {
	mc.offsetMu.Lock()
	defer mc.offsetMu.Unlock()

	if _, exists := mc.committedOffsets[topic]; !exists {
		mc.committedOffsets[topic] = make(map[int32]int64)
	}
	mc.committedOffsets[topic][partition] = offset
}

func (mc *ManagedConsumer) setProcessingOffset(topic string, partition int32, offset int64) {
	mc.offsetMu.Lock()
	defer mc.offsetMu.Unlock()

	if _, exists := mc.processingOffsets[topic]; !exists {
		mc.processingOffsets[topic] = make(map[int32]int64)
	}
	mc.processingOffsets[topic][partition] = offset
}

func (mc *ManagedConsumer) startAutoCommit() {
	mc.autoCommitTicker = time.NewTicker(mc.config.AutoCommitInterval)
	
	go func() {
		for {
			select {
			case <-mc.autoCommitTicker.C:
				ctx, cancel := context.WithTimeout(context.Background(), mc.config.Timeout)
				if err := mc.CommitProcessingOffsets(ctx); err != nil {
					log.Printf("Auto-commit failed: %v", err)
				}
				cancel()
			case <-mc.stopAutoCommit:
				return
			}
		}
	}()
}

func (mc *ManagedConsumer) incrementReceived(count int64) {
	mc.mu.Lock()
	mc.messagesReceived += count
	mc.mu.Unlock()
}

func (mc *ManagedConsumer) incrementCommitted() {
	mc.mu.Lock()
	mc.messagesCommitted++
	mc.mu.Unlock()
}

func (mc *ManagedConsumer) incrementErrors() {
	mc.mu.Lock()
	mc.errors++
	mc.mu.Unlock()
}

// ManagedConsumerMetrics holds metrics for the managed consumer
type ManagedConsumerMetrics struct {
	MessagesReceived  int64 `json:"messages_received"`
	MessagesCommitted int64 `json:"messages_committed"`
	Errors           int64 `json:"errors"`
}