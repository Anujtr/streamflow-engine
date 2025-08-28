package client

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	client    pb.MessageServiceClient
	conn      *grpc.ClientConn
	
	// Metrics
	messagesReceived int64
	errors           int64
}

type ConsumerConfig struct {
	Address       string
	Timeout       time.Duration
	ConsumerGroup string
	MaxMessages   int32
}

func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	if config.Address == "" {
		config.Address = "localhost:8080"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
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

	return &Consumer{
		client: client,
		conn:   conn,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, topic string, partition int32, offset int64, maxMessages int32) ([]*ConsumeMessage, error) {
	req := &pb.ConsumeRequest{
		Topic:         topic,
		ConsumerGroup: "default", // TODO: Make configurable
		Partition:     partition,
		Offset:        offset,
		MaxMessages:   maxMessages,
	}

	stream, err := c.client.Consume(ctx, req)
	if err != nil {
		atomic.AddInt64(&c.errors, 1)
		return nil, fmt.Errorf("failed to start consume stream: %v", err)
	}

	var messages []*ConsumeMessage
	
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			atomic.AddInt64(&c.errors, 1)
			return nil, fmt.Errorf("failed to receive message: %v", err)
		}

		if resp.Error != "" {
			atomic.AddInt64(&c.errors, 1)
			return nil, fmt.Errorf("server error: %s", resp.Error)
		}

		// Convert protobuf messages
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

		atomic.AddInt64(&c.messagesReceived, int64(len(resp.Messages)))

		// If no more messages or we got what we asked for, break
		if !resp.HasMore || int32(len(messages)) >= maxMessages {
			break
		}
	}

	return messages, nil
}

func (c *Consumer) ConsumeStream(ctx context.Context, topic string, partition int32, startOffset int64, handler MessageHandler) error {
	req := &pb.ConsumeRequest{
		Topic:         topic,
		ConsumerGroup: "default",
		Partition:     partition,
		Offset:        startOffset,
		MaxMessages:   100,
	}

	stream, err := c.client.Consume(ctx, req)
	if err != nil {
		atomic.AddInt64(&c.errors, 1)
		return fmt.Errorf("failed to start consume stream: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			atomic.AddInt64(&c.errors, 1)
			return fmt.Errorf("failed to receive message: %v", err)
		}

		if resp.Error != "" {
			atomic.AddInt64(&c.errors, 1)
			return fmt.Errorf("server error: %s", resp.Error)
		}

		// Process messages
		for _, protoMsg := range resp.Messages {
			msg := &ConsumeMessage{
				Key:       protoMsg.Key,
				Value:     protoMsg.Value,
				Partition: protoMsg.Partition,
				Offset:    protoMsg.Offset,
				Timestamp: protoMsg.Timestamp.AsTime(),
			}
			
			if err := handler(msg); err != nil {
				return fmt.Errorf("message handler error: %v", err)
			}
		}

		atomic.AddInt64(&c.messagesReceived, int64(len(resp.Messages)))
	}
}

func (c *Consumer) GetMetrics() ConsumerMetrics {
	return ConsumerMetrics{
		MessagesReceived: atomic.LoadInt64(&c.messagesReceived),
		Errors:           atomic.LoadInt64(&c.errors),
	}
}

func (c *Consumer) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

type ConsumeMessage struct {
	Key       string
	Value     []byte
	Partition int32
	Offset    int64
	Timestamp time.Time
}

type MessageHandler func(*ConsumeMessage) error

type ConsumerMetrics struct {
	MessagesReceived int64
	Errors           int64
}