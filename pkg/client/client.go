package client

import (
	"context"
	"fmt"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.MessageServiceClient
}

type Config struct {
	Address string
	Timeout time.Duration
}

func NewClient(config Config) (*Client, error) {
	if config.Address == "" {
		config.Address = "localhost:8080"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	conn, err := grpc.Dial(config.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	client := pb.NewMessageServiceClient(conn)

	return &Client{
		conn:   conn,
		client: client,
	}, nil
}

func (c *Client) Health(ctx context.Context) (*HealthInfo, error) {
	resp, err := c.client.Health(ctx, &pb.HealthRequest{})
	if err != nil {
		return nil, fmt.Errorf("health check failed: %v", err)
	}

	return &HealthInfo{
		Status:  resp.Status,
		Version: resp.Version,
		Metrics: resp.Metrics,
	}, nil
}

func (c *Client) NewProducer() (*Producer, error) {
	return &Producer{
		client: c.client,
		conn:   c.conn, // Share connection
	}, nil
}

func (c *Client) NewConsumer() (*Consumer, error) {
	return &Consumer{
		client: c.client,
		conn:   c.conn, // Share connection
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

type HealthInfo struct {
	Status  string
	Version string
	Metrics map[string]string
}