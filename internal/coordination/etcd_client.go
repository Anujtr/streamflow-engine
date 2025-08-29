package coordination

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClient wraps the etcd client with StreamFlow-specific functionality
type EtcdClient struct {
	client     *clientv3.Client
	endpoints  []string
	timeout    time.Duration
}

// EtcdConfig holds configuration for etcd client
type EtcdConfig struct {
	Endpoints   []string      `json:"endpoints"`
	Timeout     time.Duration `json:"timeout"`
	DialTimeout time.Duration `json:"dial_timeout"`
}

// NewEtcdClient creates a new etcd client wrapper
func NewEtcdClient(config EtcdConfig) (*EtcdClient, error) {
	if len(config.Endpoints) == 0 {
		config.Endpoints = []string{"localhost:2379"} // Default etcd endpoint
	}
	
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	
	if config.DialTimeout == 0 {
		config.DialTimeout = 10 * time.Second
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: config.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	return &EtcdClient{
		client:    client,
		endpoints: config.Endpoints,
		timeout:   config.Timeout,
	}, nil
}

// Close closes the etcd client connection
func (ec *EtcdClient) Close() error {
	if ec.client != nil {
		return ec.client.Close()
	}
	return nil
}

// Health checks the health of etcd cluster
func (ec *EtcdClient) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	for _, endpoint := range ec.endpoints {
		resp, err := ec.client.Status(ctx, endpoint)
		if err != nil {
			return fmt.Errorf("etcd endpoint %s is unhealthy: %v", endpoint, err)
		}
		if resp.Header.MemberId == 0 {
			return fmt.Errorf("etcd endpoint %s returned invalid member ID", endpoint)
		}
	}

	return nil
}

// Put stores a key-value pair with optional lease
func (ec *EtcdClient) Put(ctx context.Context, key, value string, leaseID clientv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	opts := []clientv3.OpOption{}
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}

	_, err := ec.client.Put(ctx, key, value, opts...)
	return err
}

// Get retrieves a value by key
func (ec *EtcdClient) Get(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	resp, err := ec.client.Get(ctx, key)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("key not found: %s", key)
	}

	return string(resp.Kvs[0].Value), nil
}

// Delete removes a key
func (ec *EtcdClient) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	_, err := ec.client.Delete(ctx, key)
	return err
}

// List returns all keys with a given prefix
func (ec *EtcdClient) List(ctx context.Context, prefix string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	resp, err := ec.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

// Watch watches for changes to a key or prefix
func (ec *EtcdClient) Watch(ctx context.Context, key string, prefix bool) clientv3.WatchChan {
	opts := []clientv3.OpOption{}
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	
	return ec.client.Watch(ctx, key, opts...)
}

// Grant creates a new lease with the given TTL
func (ec *EtcdClient) Grant(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	resp, err := ec.client.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}

	return resp.ID, nil
}

// KeepAlive keeps a lease alive
func (ec *EtcdClient) KeepAlive(ctx context.Context, leaseID clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return ec.client.KeepAlive(ctx, leaseID)
}

// Revoke revokes a lease
func (ec *EtcdClient) Revoke(ctx context.Context, leaseID clientv3.LeaseID) error {
	ctx, cancel := context.WithTimeout(ctx, ec.timeout)
	defer cancel()

	_, err := ec.client.Revoke(ctx, leaseID)
	return err
}

// GetClient returns the underlying etcd client for advanced operations
func (ec *EtcdClient) GetClient() *clientv3.Client {
	return ec.client
}