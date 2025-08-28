package api

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/storage"
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