package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/api"
	"github.com/Anujtr/streamflow-engine/internal/coordination"
	"github.com/Anujtr/streamflow-engine/internal/health"
	"github.com/Anujtr/streamflow-engine/internal/partitioning"
	"github.com/Anujtr/streamflow-engine/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const Version = "3.0.0-phase3"

func main() {
	var (
		port        = flag.String("port", "8080", "Port to listen on")
		host        = flag.String("host", "localhost", "Host to bind to")
		dataDir     = flag.String("data-dir", "./data", "Data directory for persistent storage")
		persistent  = flag.Bool("persistent", true, "Enable persistent storage with Pebble")
		enableEtcd  = flag.Bool("etcd", false, "Enable etcd for leader election")
		etcdEndpoints = flag.String("etcd-endpoints", "localhost:2379", "Comma-separated etcd endpoints")
		nodeID      = flag.String("node-id", "node-1", "Node ID for leader election")
	)
	flag.Parse()

	_ = context.Background() // Available for future use

	// Create storage (persistent or in-memory)
	var store *storage.Storage
	var err error
	
	if *persistent {
		config := storage.StorageConfig{
			DataDir:         *dataDir,
			PersistenceMode: true,
		}
		store, err = storage.NewPersistentStorage(config)
		if err != nil {
			log.Fatalf("Failed to create persistent storage: %v", err)
		}
		log.Printf("Using persistent storage in %s", *dataDir)
	} else {
		store = storage.NewStorage()
		log.Println("Using in-memory storage")
	}

	// Create health monitor
	healthMonitor := health.NewHealthMonitor(health.HealthMonitorConfig{
		NodeID:        *nodeID,
		CheckInterval: 30 * time.Second,
		CheckTimeout:  5 * time.Second,
	})
	healthMonitor.RegisterStorage(store)
	healthMonitor.Start()

	// Create partition manager with optional leader election
	var partitionManager *partitioning.PartitionManager
	var etcdClient *coordination.EtcdClient
	var leadershipManager *coordination.LeadershipManager

	if *enableEtcd {
		// Initialize etcd client
		etcdConfig := coordination.EtcdConfig{
			Endpoints: []string{*etcdEndpoints},
		}
		etcdClient, err = coordination.NewEtcdClient(etcdConfig)
		if err != nil {
			log.Fatalf("Failed to create etcd client: %v", err)
		}

		// Register etcd for health monitoring
		healthMonitor.RegisterEtcd(etcdClient)

		// Create leadership manager
		leadershipManager = coordination.NewLeadershipManager(etcdClient, *nodeID)

		// Create partition manager with leadership
		partitionManager = partitioning.NewPartitionManagerWithLeadership(store, leadershipManager, *nodeID)
		log.Printf("Using distributed mode with etcd and leader election (node: %s)", *nodeID)
	} else {
		partitionManager = partitioning.NewPartitionManager(store)
		log.Println("Using single-node mode")
	}

	// Create consumer group coordinator
	consumerCoordinator := coordination.NewConsumerGroupCoordinator()

	// Create servers
	messageServer := api.NewServer(store, Version)
	partitionServer := api.NewPartitionServer(partitionManager)
	consumerGroupServer := api.NewConsumerGroupServer(consumerCoordinator)

	// Create gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterMessageServiceServer(grpcServer, messageServer)
	pb.RegisterPartitionServiceServer(grpcServer, partitionServer)
	pb.RegisterConsumerGroupServiceServer(grpcServer, consumerGroupServer)

	// Enable reflection for easier debugging
	reflection.Register(grpcServer)

	// Listen on the specified address
	address := fmt.Sprintf("%s:%s", *host, *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	log.Printf("StreamFlow Engine v%s starting on %s", Version, address)

	// Start server in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("Failed to serve: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Graceful shutdown sequence
	log.Println("Stopping health monitor...")
	healthMonitor.Stop()

	log.Println("Stopping partition manager...")
	partitionManager.Stop()

	if leadershipManager != nil {
		log.Println("Stopping leadership manager...")
		leadershipManager.Stop()
	}

	log.Println("Stopping consumer coordinator...")
	consumerCoordinator.Stop()

	if etcdClient != nil {
		log.Println("Closing etcd client...")
		etcdClient.Close()
	}

	log.Println("Closing storage...")
	if err := store.Close(); err != nil {
		log.Printf("Error closing storage: %v", err)
	}
	
	log.Println("Stopping gRPC server...")
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("Server stopped gracefully")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout exceeded, forcing stop")
		grpcServer.Stop()
	}
}

// CreateTestTopic creates a test topic for demonstration
func CreateTestTopic(store *storage.Storage) {
	err := store.CreateTopic("test-topic", 4)
	if err != nil {
		log.Printf("Failed to create test topic: %v", err)
		return
	}
	log.Println("Created test topic with 4 partitions")
}