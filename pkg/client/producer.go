package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Producer struct {
	client      pb.MessageServiceClient
	conn        *grpc.ClientConn
	
	// Metrics
	messagesSent int64
	errors       int64
}

type ProducerConfig struct {
	Address     string
	Timeout     time.Duration
	BatchSize   int
	Compression bool
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	if config.Address == "" {
		config.Address = "localhost:8080"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	// Connect to the server
	conn, err := grpc.Dial(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := pb.NewMessageServiceClient(conn)

	return &Producer{
		client: client,
		conn:   conn,
	}, nil
}

type Message struct {
	Key   string
	Value []byte
}

func (p *Producer) Send(ctx context.Context, topic string, messages ...Message) ([]*ProduceResult, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("at least one message is required")
	}

	// Convert to protobuf messages
	protoMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		protoMessages[i] = &pb.Message{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}

	// Send to server
	req := &pb.ProduceRequest{
		Topic:    topic,
		Messages: protoMessages,
	}

	resp, err := p.client.Produce(ctx, req)
	if err != nil {
		atomic.AddInt64(&p.errors, 1)
		return nil, fmt.Errorf("failed to produce messages: %v", err)
	}

	// Convert results
	results := make([]*ProduceResult, len(resp.Results))
	for i, result := range resp.Results {
		results[i] = &ProduceResult{
			Partition: result.Partition,
			Offset:    result.Offset,
			Error:     result.Error,
		}
		
		if result.Error == "" {
			atomic.AddInt64(&p.messagesSent, 1)
		} else {
			atomic.AddInt64(&p.errors, 1)
		}
	}

	return results, nil
}

func (p *Producer) SendSingle(ctx context.Context, topic, key string, value []byte) (*ProduceResult, error) {
	results, err := p.Send(ctx, topic, Message{Key: key, Value: value})
	if err != nil {
		return nil, err
	}
	
	if len(results) == 0 {
		return nil, fmt.Errorf("no results returned")
	}
	
	return results[0], nil
}

func (p *Producer) GetMetrics() ProducerMetrics {
	return ProducerMetrics{
		MessagesSent: atomic.LoadInt64(&p.messagesSent),
		Errors:       atomic.LoadInt64(&p.errors),
	}
}

func (p *Producer) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

type ProduceResult struct {
	Partition int32
	Offset    int64
	Error     string
}

type ProducerMetrics struct {
	MessagesSent int64
	Errors       int64
}