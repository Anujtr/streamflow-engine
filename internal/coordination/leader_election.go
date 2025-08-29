package coordination

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// PartitionLeader manages leader election for a single partition
type PartitionLeader struct {
	etcdClient    *EtcdClient
	nodeID        string
	topic         string
	partition     int32
	leaseID       clientv3.LeaseID
	isLeader      bool
	onLeadershipChange func(bool)
	cancelFunc    context.CancelFunc
	mu            sync.RWMutex
	leaseTTL      int64
}

// LeaderElectionConfig holds configuration for leader election
type LeaderElectionConfig struct {
	NodeID              string                 `json:"node_id"`
	LeaseTTL            int64                  `json:"lease_ttl"` // seconds
	OnLeadershipChange  func(bool)             `json:"-"`
}

// NewPartitionLeader creates a new partition leader for leader election
func NewPartitionLeader(etcdClient *EtcdClient, topic string, partition int32, config LeaderElectionConfig) *PartitionLeader {
	if config.LeaseTTL == 0 {
		config.LeaseTTL = 10 // Default 10 seconds
	}

	return &PartitionLeader{
		etcdClient:         etcdClient,
		nodeID:             config.NodeID,
		topic:              topic,
		partition:          partition,
		onLeadershipChange: config.OnLeadershipChange,
		leaseTTL:           config.LeaseTTL,
	}
}

// Start begins the leader election process
func (pl *PartitionLeader) Start(ctx context.Context) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if pl.cancelFunc != nil {
		return fmt.Errorf("leader election already started")
	}

	electionCtx, cancel := context.WithCancel(ctx)
	pl.cancelFunc = cancel

	go pl.runLeaderElection(electionCtx)
	return nil
}

// Stop stops the leader election process
func (pl *PartitionLeader) Stop() {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if pl.cancelFunc != nil {
		pl.cancelFunc()
		pl.cancelFunc = nil
	}

	// Revoke lease if we have one
	if pl.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pl.etcdClient.Revoke(ctx, pl.leaseID)
		pl.leaseID = 0
	}

	pl.setLeader(false)
}

// IsLeader returns whether this node is the leader for the partition
func (pl *PartitionLeader) IsLeader() bool {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.isLeader
}

// GetLeader returns the current leader node ID for the partition
func (pl *PartitionLeader) GetLeader(ctx context.Context) (string, error) {
	key := pl.getLeaderKey()
	
	leaderID, err := pl.etcdClient.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("failed to get leader: %v", err)
	}

	return leaderID, nil
}

// runLeaderElection runs the leader election loop
func (pl *PartitionLeader) runLeaderElection(ctx context.Context) {
	defer func() {
		pl.setLeader(false)
		if pl.leaseID != 0 {
			revCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pl.etcdClient.Revoke(revCtx, pl.leaseID)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := pl.tryBecomeLeader(ctx); err != nil {
			log.Printf("Leader election error for %s partition %d: %v", pl.topic, pl.partition, err)
			time.Sleep(time.Second)
			continue
		}

		// If we became leader, maintain leadership
		if pl.IsLeader() {
			pl.maintainLeadership(ctx)
		}

		// Wait before retrying
		time.Sleep(time.Second)
	}
}

// tryBecomeLeader attempts to become the leader for the partition
func (pl *PartitionLeader) tryBecomeLeader(ctx context.Context) error {
	// Create a lease
	leaseID, err := pl.etcdClient.Grant(ctx, pl.leaseTTL)
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}

	key := pl.getLeaderKey()
	
	// Try to acquire leadership using transaction
	client := pl.etcdClient.GetClient()
	txnResp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, pl.nodeID, clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(key)).
		Commit()

	if err != nil {
		// Clean up lease on error
		pl.etcdClient.Revoke(ctx, leaseID)
		return fmt.Errorf("failed to execute transaction: %v", err)
	}

	if txnResp.Succeeded {
		// We became the leader
		pl.mu.Lock()
		pl.leaseID = leaseID
		pl.mu.Unlock()
		pl.setLeader(true)
		log.Printf("Node %s became leader for %s partition %d", pl.nodeID, pl.topic, pl.partition)
		return nil
	}

	// Leadership acquisition failed, clean up lease
	pl.etcdClient.Revoke(ctx, leaseID)
	
	// Watch for leadership changes
	return pl.watchLeadership(ctx, key)
}

// maintainLeadership keeps the leadership lease alive
func (pl *PartitionLeader) maintainLeadership(ctx context.Context) {
	pl.mu.RLock()
	leaseID := pl.leaseID
	pl.mu.RUnlock()

	if leaseID == 0 {
		return
	}

	// Keep lease alive
	keepAliveCh, err := pl.etcdClient.KeepAlive(ctx, leaseID)
	if err != nil {
		log.Printf("Failed to keep lease alive: %v", err)
		pl.setLeader(false)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-keepAliveCh:
			if resp == nil {
				// Lease expired
				log.Printf("Lease expired for %s partition %d", pl.topic, pl.partition)
				pl.setLeader(false)
				pl.mu.Lock()
				pl.leaseID = 0
				pl.mu.Unlock()
				return
			}
		}
	}
}

// watchLeadership watches for changes in leadership
func (pl *PartitionLeader) watchLeadership(ctx context.Context, key string) error {
	watchCh := pl.etcdClient.Watch(ctx, key, false)
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchResp := <-watchCh:
			if watchResp.Err() != nil {
				return fmt.Errorf("watch error: %v", watchResp.Err())
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					// Leadership is available
					log.Printf("Leadership available for %s partition %d", pl.topic, pl.partition)
					return nil
				}
			}
		}
	}
}

// setLeader sets the leadership status and notifies callback
func (pl *PartitionLeader) setLeader(isLeader bool) {
	pl.mu.Lock()
	changed := pl.isLeader != isLeader
	pl.isLeader = isLeader
	pl.mu.Unlock()

	if changed && pl.onLeadershipChange != nil {
		pl.onLeadershipChange(isLeader)
	}
}

// getLeaderKey returns the etcd key for partition leadership
func (pl *PartitionLeader) getLeaderKey() string {
	return fmt.Sprintf("/streamflow/leader/%s/%d", pl.topic, pl.partition)
}

// LeadershipManager manages leader election for multiple partitions
type LeadershipManager struct {
	etcdClient *EtcdClient
	nodeID     string
	leaders    map[string]*PartitionLeader
	mu         sync.RWMutex
}

// NewLeadershipManager creates a new leadership manager
func NewLeadershipManager(etcdClient *EtcdClient, nodeID string) *LeadershipManager {
	return &LeadershipManager{
		etcdClient: etcdClient,
		nodeID:     nodeID,
		leaders:    make(map[string]*PartitionLeader),
	}
}

// StartLeaderElection starts leader election for a partition
func (lm *LeadershipManager) StartLeaderElection(ctx context.Context, topic string, partition int32, config LeaderElectionConfig) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if _, exists := lm.leaders[key]; exists {
		return fmt.Errorf("leader election already started for %s partition %d", topic, partition)
	}

	config.NodeID = lm.nodeID
	leader := NewPartitionLeader(lm.etcdClient, topic, partition, config)
	lm.leaders[key] = leader

	return leader.Start(ctx)
}

// StopLeaderElection stops leader election for a partition
func (lm *LeadershipManager) StopLeaderElection(topic string, partition int32) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if leader, exists := lm.leaders[key]; exists {
		leader.Stop()
		delete(lm.leaders, key)
	}
}

// IsLeader returns whether this node is the leader for a partition
func (lm *LeadershipManager) IsLeader(topic string, partition int32) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if leader, exists := lm.leaders[key]; exists {
		return leader.IsLeader()
	}
	return false
}

// GetLeader returns the leader node ID for a partition
func (lm *LeadershipManager) GetLeader(ctx context.Context, topic string, partition int32) (string, error) {
	lm.mu.RLock()
	leader, exists := lm.leaders[fmt.Sprintf("%s-%d", topic, partition)]
	lm.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("no leader election started for %s partition %d", topic, partition)
	}

	return leader.GetLeader(ctx)
}

// Stop stops all leader elections
func (lm *LeadershipManager) Stop() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, leader := range lm.leaders {
		leader.Stop()
	}
	lm.leaders = make(map[string]*PartitionLeader)
}