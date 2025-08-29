package health

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/coordination"
	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// ComponentHealth represents the health of a system component
type ComponentHealth struct {
	Name         string            `json:"name"`
	Status       HealthStatus      `json:"status"`
	LastChecked  time.Time         `json:"last_checked"`
	Error        string            `json:"error,omitempty"`
	Details      map[string]string `json:"details,omitempty"`
}

// SystemHealth represents the overall system health
type SystemHealth struct {
	Status     HealthStatus                `json:"status"`
	Timestamp  time.Time                   `json:"timestamp"`
	Components map[string]*ComponentHealth `json:"components"`
	Uptime     time.Duration               `json:"uptime"`
	NodeID     string                      `json:"node_id"`
}

// HealthMonitor monitors the health of various system components
type HealthMonitor struct {
	nodeID       string
	startTime    time.Time
	storage      *storage.Storage
	etcdClient   *coordination.EtcdClient
	components   map[string]*ComponentHealth
	mu           sync.RWMutex
	
	// Health check intervals
	checkInterval time.Duration
	checkTimeout  time.Duration
	
	// Shutdown
	ctx        context.Context
	cancel     context.CancelFunc
	stopped    int32
}

// HealthMonitorConfig holds configuration for the health monitor
type HealthMonitorConfig struct {
	NodeID        string        `json:"node_id"`
	CheckInterval time.Duration `json:"check_interval"`
	CheckTimeout  time.Duration `json:"check_timeout"`
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config HealthMonitorConfig) *HealthMonitor {
	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.CheckTimeout == 0 {
		config.CheckTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &HealthMonitor{
		nodeID:        config.NodeID,
		startTime:     time.Now(),
		components:    make(map[string]*ComponentHealth),
		checkInterval: config.CheckInterval,
		checkTimeout:  config.CheckTimeout,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// RegisterStorage registers storage component for health monitoring
func (hm *HealthMonitor) RegisterStorage(storage *storage.Storage) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	
	hm.storage = storage
	hm.components["storage"] = &ComponentHealth{
		Name:        "storage",
		Status:      HealthStatusHealthy,
		LastChecked: time.Now(),
		Details:     make(map[string]string),
	}
}

// RegisterEtcd registers etcd client for health monitoring
func (hm *HealthMonitor) RegisterEtcd(etcdClient *coordination.EtcdClient) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	
	hm.etcdClient = etcdClient
	hm.components["etcd"] = &ComponentHealth{
		Name:        "etcd",
		Status:      HealthStatusHealthy,
		LastChecked: time.Now(),
		Details:     make(map[string]string),
	}
}

// Start starts the health monitoring
func (hm *HealthMonitor) Start() {
	go hm.runHealthChecks()
}

// Stop stops the health monitoring
func (hm *HealthMonitor) Stop() {
	if atomic.CompareAndSwapInt32(&hm.stopped, 0, 1) {
		hm.cancel()
	}
}

// GetHealth returns the current system health
func (hm *HealthMonitor) GetHealth() *SystemHealth {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	// Deep copy components
	components := make(map[string]*ComponentHealth)
	for name, health := range hm.components {
		components[name] = &ComponentHealth{
			Name:        health.Name,
			Status:      health.Status,
			LastChecked: health.LastChecked,
			Error:       health.Error,
			Details:     make(map[string]string),
		}
		for k, v := range health.Details {
			components[name].Details[k] = v
		}
	}

	// Determine overall status
	overallStatus := hm.determineOverallStatus(components)

	return &SystemHealth{
		Status:     overallStatus,
		Timestamp:  time.Now(),
		Components: components,
		Uptime:     time.Since(hm.startTime),
		NodeID:     hm.nodeID,
	}
}

// runHealthChecks runs periodic health checks
func (hm *HealthMonitor) runHealthChecks() {
	ticker := time.NewTicker(hm.checkInterval)
	defer ticker.Stop()

	// Run initial health check
	hm.performHealthChecks()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.performHealthChecks()
		}
	}
}

// performHealthChecks performs health checks on all registered components
func (hm *HealthMonitor) performHealthChecks() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Check storage health
	if hm.storage != nil {
		hm.checkStorageHealth()
	}

	// Check etcd health
	if hm.etcdClient != nil {
		hm.checkEtcdHealth()
	}
}

// checkStorageHealth checks the health of the storage component
func (hm *HealthMonitor) checkStorageHealth() {
	component := hm.components["storage"]
	if component == nil {
		return
	}

	ctx, cancel := context.WithTimeout(hm.ctx, hm.checkTimeout)
	defer cancel()

	// Try to list topics as a basic health check
	topics := hm.storage.ListTopics()
	
	component.LastChecked = time.Now()
	component.Status = HealthStatusHealthy
	component.Error = ""
	component.Details["topic_count"] = fmt.Sprintf("%d", len(topics))
	
	// Additional check for persistent storage
	if offsetStore := hm.storage.GetOffsetStore(); offsetStore != nil {
		component.Details["persistence"] = "enabled"
	} else {
		component.Details["persistence"] = "disabled"
	}

	select {
	case <-ctx.Done():
		component.Status = HealthStatusDegraded
		component.Error = "health check timed out"
	default:
		// Health check completed successfully
	}
}

// checkEtcdHealth checks the health of the etcd component
func (hm *HealthMonitor) checkEtcdHealth() {
	component := hm.components["etcd"]
	if component == nil {
		return
	}

	ctx, cancel := context.WithTimeout(hm.ctx, hm.checkTimeout)
	defer cancel()

	err := hm.etcdClient.Health(ctx)
	
	component.LastChecked = time.Now()
	if err != nil {
		component.Status = HealthStatusUnhealthy
		component.Error = err.Error()
	} else {
		component.Status = HealthStatusHealthy
		component.Error = ""
	}
}

// determineOverallStatus determines the overall system status based on component statuses
func (hm *HealthMonitor) determineOverallStatus(components map[string]*ComponentHealth) HealthStatus {
	if len(components) == 0 {
		return HealthStatusHealthy
	}

	hasUnhealthy := false
	hasDegraded := false

	for _, component := range components {
		switch component.Status {
		case HealthStatusUnhealthy:
			hasUnhealthy = true
		case HealthStatusDegraded:
			hasDegraded = true
		}
	}

	if hasUnhealthy {
		return HealthStatusUnhealthy
	}
	if hasDegraded {
		return HealthStatusDegraded
	}
	
	return HealthStatusHealthy
}