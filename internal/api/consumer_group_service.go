package api

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/coordination"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ConsumerGroupServer implements the ConsumerGroupService
type ConsumerGroupServer struct {
	pb.UnimplementedConsumerGroupServiceServer
	coordinator *coordination.ConsumerGroupCoordinator
}

// NewConsumerGroupServer creates a new consumer group service server
func NewConsumerGroupServer(coordinator *coordination.ConsumerGroupCoordinator) *ConsumerGroupServer {
	return &ConsumerGroupServer{
		coordinator: coordinator,
	}
}

// JoinGroup adds a consumer to a consumer group
func (cgs *ConsumerGroupServer) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id is required")
	}

	if req.GroupId == "" {
		return nil, fmt.Errorf("group_id is required")
	}

	if len(req.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	sessionTimeout := time.Duration(req.SessionTimeoutMs) * time.Millisecond
	if sessionTimeout <= 0 {
		sessionTimeout = 30 * time.Second // Default session timeout
	}

	// Join the group
	err := cgs.coordinator.JoinGroup(req.ConsumerId, req.GroupId, req.Topics, sessionTimeout)
	if err != nil {
		return &pb.JoinGroupResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to join group: %v", err),
		}, nil
	}

	// Get assignment (might be empty initially during rebalancing)
	assignment, err := cgs.coordinator.GetGroupAssignment(req.ConsumerId)
	if err != nil {
		// Consumer joined but assignment not ready yet
		assignment = []int32{}
	}

	return &pb.JoinGroupResponse{
		Success:            true,
		Message:            fmt.Sprintf("Successfully joined group %s", req.GroupId),
		AssignedPartitions: assignment,
		GroupState:         "Rebalancing", // Initial state
	}, nil
}

// LeaveGroup removes a consumer from a consumer group
func (cgs *ConsumerGroupServer) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id is required")
	}

	err := cgs.coordinator.LeaveGroup(req.ConsumerId)
	if err != nil {
		return &pb.LeaveGroupResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to leave group: %v", err),
		}, nil
	}

	return &pb.LeaveGroupResponse{
		Success: true,
		Message: "Successfully left group",
	}, nil
}

// Heartbeat updates the consumer's heartbeat
func (cgs *ConsumerGroupServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id is required")
	}

	err := cgs.coordinator.Heartbeat(req.ConsumerId)
	if err != nil {
		return &pb.HeartbeatResponse{
			Success: false,
			Message: fmt.Sprintf("Heartbeat failed: %v", err),
		}, nil
	}

	return &pb.HeartbeatResponse{
		Success:         true,
		Message:         "Heartbeat received",
		ShouldRebalance: false, // For simplicity, always false in Phase 2
	}, nil
}

// GetGroupInfo returns information about a consumer group
func (cgs *ConsumerGroupServer) GetGroupInfo(ctx context.Context, req *pb.GroupInfoRequest) (*pb.GroupInfoResponse, error) {
	if req.GroupId == "" {
		return nil, fmt.Errorf("group_id is required")
	}

	groupInfo, err := cgs.coordinator.GetGroupInfo(req.GroupId)
	if err != nil {
		return nil, fmt.Errorf("failed to get group info: %v", err)
	}

	// Convert members
	pbMembers := make([]*pb.ConsumerMember, len(groupInfo.Members))
	for i, member := range groupInfo.Members {
		pbMembers[i] = &pb.ConsumerMember{
			ConsumerId:         member.ID,
			AssignedPartitions: member.AssignedPartitions,
			State:              member.State.String(),
			LastHeartbeat:      timestamppb.New(member.LastHeartbeat),
			Metadata:           member.Metadata,
		}
	}

	return &pb.GroupInfoResponse{
		GroupId:       groupInfo.ID,
		State:         groupInfo.State.String(),
		Members:       pbMembers,
		Topics:        groupInfo.Topics,
		CreatedAt:     timestamppb.New(groupInfo.CreatedAt),
		LastRebalance: timestamppb.New(groupInfo.LastRebalance),
	}, nil
}