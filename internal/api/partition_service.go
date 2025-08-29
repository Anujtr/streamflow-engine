package api

import (
	"context"
	"fmt"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/partitioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PartitionServer implements the PartitionService
type PartitionServer struct {
	pb.UnimplementedPartitionServiceServer
	partitionManager *partitioning.PartitionManager
}

// NewPartitionServer creates a new partition service server
func NewPartitionServer(pm *partitioning.PartitionManager) *PartitionServer {
	return &PartitionServer{
		partitionManager: pm,
	}
}

// ListPartitions lists all partitions for a topic
func (ps *PartitionServer) ListPartitions(ctx context.Context, req *pb.ListPartitionsRequest) (*pb.ListPartitionsResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	topicPartitions, err := ps.partitionManager.GetTopicPartitions(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic partitions: %v", err)
	}

	metrics, err := ps.partitionManager.GetAllPartitionMetrics(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition metrics: %v", err)
	}

	partitionInfos := make([]*pb.PartitionInfo, 0, len(metrics))
	for _, metric := range metrics {
		partitionInfos = append(partitionInfos, &pb.PartitionInfo{
			PartitionId:   metric.PartitionID,
			MessageCount:  metric.MessageCount,
			BytesStored:   metric.BytesStored,
			LastActivity:  timestamppb.New(metric.LastActivity),
			Leader:        "localhost", // For single-node deployment
		})
	}

	return &pb.ListPartitionsResponse{
		Partitions: partitionInfos,
	}, nil
}

// RebalancePartitions triggers a partition rebalance
func (ps *PartitionServer) RebalancePartitions(ctx context.Context, req *pb.RebalanceRequest) (*pb.RebalanceResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	// For Phase 2, we'll implement a simple rebalance by returning current state
	// In a real distributed system, this would coordinate rebalancing across nodes
	
	topicPartitions, err := ps.partitionManager.GetTopicPartitions(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic partitions: %v", err)
	}

	assignments := make(map[int32]string)
	partitions := topicPartitions.HashRing.GetPartitions()
	for _, partition := range partitions {
		assignments[partition] = "localhost" // Single-node assignment
	}

	return &pb.RebalanceResponse{
		Success:              true,
		Message:              fmt.Sprintf("Rebalanced %d partitions for topic %s", len(assignments), req.Topic),
		PartitionAssignments: assignments,
	}, nil
}

// GetPartitionMetrics returns metrics for partitions
func (ps *PartitionServer) GetPartitionMetrics(ctx context.Context, req *pb.PartitionMetricsRequest) (*pb.PartitionMetricsResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	var metrics []*partitioning.PartitionMetrics
	var err error

	if req.PartitionId == -1 {
		// Get all partition metrics
		metrics, err = ps.partitionManager.GetAllPartitionMetrics(req.Topic)
	} else {
		// Get specific partition metrics
		metric, getErr := ps.partitionManager.GetPartitionMetrics(req.Topic, req.PartitionId)
		if getErr != nil {
			return nil, fmt.Errorf("failed to get partition metrics: %v", getErr)
		}
		metrics = []*partitioning.PartitionMetrics{metric}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get partition metrics: %v", err)
	}

	pbMetrics := make([]*pb.PartitionMetrics, len(metrics))
	for i, metric := range metrics {
		pbMetrics[i] = &pb.PartitionMetrics{
			PartitionId:   metric.PartitionID,
			MessageCount:  metric.MessageCount,
			BytesStored:   metric.BytesStored,
			ProduceRate:   metric.ProduceRate,
			ConsumeRate:   metric.ConsumeRate,
			LastActivity:  timestamppb.New(metric.LastActivity),
		}
	}

	return &pb.PartitionMetricsResponse{
		Metrics: pbMetrics,
	}, nil
}

// ScalePartitions scales the number of partitions for a topic
func (ps *PartitionServer) ScalePartitions(ctx context.Context, req *pb.ScalePartitionsRequest) (*pb.ScalePartitionsResponse, error) {
	if req.Topic == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	if req.NewPartitionCount <= 0 {
		return nil, fmt.Errorf("new partition count must be positive")
	}

	// Get current partition count
	topicPartitions, err := ps.partitionManager.GetTopicPartitions(req.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic partitions: %v", err)
	}

	oldCount := topicPartitions.PartitionCount

	// Scale partitions
	err = ps.partitionManager.ScalePartitions(req.Topic, req.NewPartitionCount)
	if err != nil {
		return &pb.ScalePartitionsResponse{
			Success:  false,
			Message:  fmt.Sprintf("Failed to scale partitions: %v", err),
			OldCount: oldCount,
			NewCount: oldCount,
		}, nil
	}

	return &pb.ScalePartitionsResponse{
		Success:  true,
		Message:  fmt.Sprintf("Successfully scaled topic %s from %d to %d partitions", req.Topic, oldCount, req.NewPartitionCount),
		OldCount: oldCount,
		NewCount: req.NewPartitionCount,
	}, nil
}