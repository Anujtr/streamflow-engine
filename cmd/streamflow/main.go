package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"github.com/Anujtr/streamflow-engine/internal/api"
	"github.com/Anujtr/streamflow-engine/internal/coordination"
	"github.com/Anujtr/streamflow-engine/internal/partitioning"
	"github.com/Anujtr/streamflow-engine/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const Version = "2.0.0-phase2"

func main() {
	var (
		port = flag.String("port", "8080", "Port to listen on")
		host = flag.String("host", "localhost", "Host to bind to")
	)
	flag.Parse()

	// Create storage
	store := storage.NewStorage()

	// Create partition manager
	partitionManager := partitioning.NewPartitionManager(store)

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

	// Graceful shutdown
	log.Println("Stopping consumer coordinator...")
	consumerCoordinator.Stop()
	
	log.Println("Stopping gRPC server...")
	grpcServer.GracefulStop()
	log.Println("Server stopped")
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