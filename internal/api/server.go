package api

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedMessageServiceServer
	storage     *storage.Storage
	version     string
	
	// Metrics
	messagesProduced int64
	messagesConsumed int64
	activeConsumers  int64
}

func NewServer(storage *storage.Storage, version string) *Server {
	return &Server{
		storage: storage,
		version: version,
	}
}

func (s *Server) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	if len(req.Messages) == 0 {
		return nil, fmt.Errorf("at least one message is required")
	}

	// Ensure topic exists (create if it doesn't)
	_, err := s.storage.GetTopic(req.Topic)
	if err != nil {
		// Create topic with default 4 partitions
		if createErr := s.storage.CreateTopic(req.Topic, 4); createErr != nil {
			return nil, fmt.Errorf("failed to create topic: %v", createErr)
		}
	}

	results := make([]*pb.ProduceResult, len(req.Messages))
	
	for i, protoMsg := range req.Messages {
		msg := storage.FromProto(protoMsg)
		partition, offset, err := s.storage.Produce(req.Topic, msg)
		
		result := &pb.ProduceResult{
			Partition: partition,
			Offset:    offset,
		}
		
		if err != nil {
			result.Error = err.Error()
			log.Printf("Failed to produce message %d: %v", i, err)
		} else {
			atomic.AddInt64(&s.messagesProduced, 1)
		}
		
		results[i] = result
	}

	return &pb.ProduceResponse{
		Results: results,
	}, nil
}

func (s *Server) Consume(req *pb.ConsumeRequest, stream pb.MessageService_ConsumeServer) error {
	if req.Topic == "" {
		return fmt.Errorf("topic name is required")
	}

	if req.MaxMessages <= 0 {
		req.MaxMessages = 100 // Default batch size
	}

	atomic.AddInt64(&s.activeConsumers, 1)
	defer atomic.AddInt64(&s.activeConsumers, -1)

	ctx := stream.Context()
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, hasMore, err := s.storage.Consume(req.Topic, req.Partition, req.Offset, req.MaxMessages)
		if err != nil {
			return stream.Send(&pb.ConsumeResponse{
				Error: err.Error(),
			})
		}

		if len(messages) > 0 {
			protoMessages := make([]*pb.Message, len(messages))
			for i, msg := range messages {
				protoMessages[i] = msg.ToProto()
			}

			response := &pb.ConsumeResponse{
				Messages: protoMessages,
				HasMore:  hasMore,
			}

			if err := stream.Send(response); err != nil {
				return err
			}

			atomic.AddInt64(&s.messagesConsumed, int64(len(messages)))
			req.Offset += int64(len(messages))
		}

		if !hasMore {
			// No more messages, wait a bit before checking again
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
	}
}

func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	metrics := map[string]string{
		"messages_produced":  fmt.Sprintf("%d", atomic.LoadInt64(&s.messagesProduced)),
		"messages_consumed":  fmt.Sprintf("%d", atomic.LoadInt64(&s.messagesConsumed)),
		"active_consumers":   fmt.Sprintf("%d", atomic.LoadInt64(&s.activeConsumers)),
		"topics_count":       fmt.Sprintf("%d", len(s.storage.ListTopics())),
		"uptime":            time.Since(startTime).String(),
	}

	return &pb.HealthResponse{
		Status:  "healthy",
		Version: s.version,
		Metrics: metrics,
	}, nil
}

var startTime = time.Now()

// ConsumeBatch consumes messages in a single request (non-streaming)
func (s *Server) ConsumeBatch(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	if req.Topic == "" {
		return &pb.ConsumeResponse{
			Error: "topic name is required",
		}, nil
	}

	messages, hasMore, err := s.storage.Consume(req.Topic, req.Partition, req.Offset, req.MaxMessages)
	if err != nil {
		return &pb.ConsumeResponse{
			Error: fmt.Sprintf("failed to consume messages: %v", err),
		}, nil
	}

	// Convert to protobuf messages
	pbMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		pbMessages[i] = &pb.Message{
			Key:       msg.Key,
			Value:     msg.Value,
			Partition: req.Partition,
			Offset:    req.Offset + int64(i),
			Timestamp: timestamppb.New(time.Now()),
		}
	}

	return &pb.ConsumeResponse{
		Messages: pbMessages,
		HasMore:  hasMore,
	}, nil
}

// CommitOffset commits the offset for a consumer group
func (s *Server) CommitOffset(ctx context.Context, req *pb.CommitOffsetRequest) (*pb.CommitOffsetResponse, error) {
	if req.ConsumerGroup == "" {
		return &pb.CommitOffsetResponse{
			Success: false,
			Error:   "consumer group is required",
		}, nil
	}
	if req.Topic == "" {
		return &pb.CommitOffsetResponse{
			Success: false,
			Error:   "topic is required",
		}, nil
	}

	// Get offset store if available
	offsetStore := s.storage.GetOffsetStore()
	if offsetStore == nil {
		return &pb.CommitOffsetResponse{
			Success: false,
			Error:   "offset management not available (requires persistent storage)",
		}, nil
	}

	err := offsetStore.CommitOffset(req.ConsumerGroup, req.Topic, req.Partition, req.Offset)
	if err != nil {
		return &pb.CommitOffsetResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to commit offset: %v", err),
		}, nil
	}

	return &pb.CommitOffsetResponse{
		Success: true,
	}, nil
}

// GetOffset retrieves the committed offset for a consumer group
func (s *Server) GetOffset(ctx context.Context, req *pb.GetOffsetRequest) (*pb.GetOffsetResponse, error) {
	if req.ConsumerGroup == "" {
		return &pb.GetOffsetResponse{
			Error: "consumer group is required",
		}, nil
	}
	if req.Topic == "" {
		return &pb.GetOffsetResponse{
			Error: "topic is required",
		}, nil
	}

	// Get offset store if available
	offsetStore := s.storage.GetOffsetStore()
	if offsetStore == nil {
		// Return offset 0 if no persistent storage
		return &pb.GetOffsetResponse{
			Offset: 0,
		}, nil
	}

	offset, err := offsetStore.GetOffset(req.ConsumerGroup, req.Topic, req.Partition)
	if err != nil {
		return &pb.GetOffsetResponse{
			Error: fmt.Sprintf("failed to get offset: %v", err),
		}, nil
	}

	return &pb.GetOffsetResponse{
		Offset: offset,
	}, nil
}