package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
	"github.com/Anujtr/streamflow-engine/internal/stream"
	"github.com/gorilla/websocket"
)

// Event represents an e-commerce event from the simulator
type Event struct {
	EventID          string                 `json:"event_id"`
	EventType        string                 `json:"event_type"`
	UserID           string                 `json:"user_id"`
	SessionID        string                 `json:"session_id"`
	Timestamp        time.Time              `json:"timestamp"`
	ProductID        *string                `json:"product_id,omitempty"`
	ProductName      *string                `json:"product_name,omitempty"`
	ProductCategory  *string                `json:"product_category,omitempty"`
	ProductPrice     *float64               `json:"product_price,omitempty"`
	Quantity         int                    `json:"quantity"`
	SearchQuery      *string                `json:"search_query,omitempty"`
	SearchResults    *int                   `json:"search_results_count,omitempty"`
	PageURL          *string                `json:"page_url,omitempty"`
	Location         string                 `json:"location"`
	DeviceType       string                 `json:"device_type"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// Analytics results structures
type SalesMetrics struct {
	TotalRevenue       float64            `json:"total_revenue"`
	TransactionCount   int64              `json:"transaction_count"`
	AverageOrderValue  float64            `json:"average_order_value"`
	TopProducts        []ProductMetric    `json:"top_products"`
	SalesByCategory    []CategoryMetric   `json:"sales_by_category"`
	SalesByLocation    []LocationMetric   `json:"sales_by_location"`
	RevenueTimeline    []TimelinePoint    `json:"revenue_timeline"`
	LastUpdated        time.Time          `json:"last_updated"`
}

type ProductMetric struct {
	ProductID    string  `json:"product_id"`
	ProductName  string  `json:"product_name"`
	Revenue      float64 `json:"revenue"`
	Units        int64   `json:"units"`
}

type CategoryMetric struct {
	Category string  `json:"category"`
	Revenue  float64 `json:"revenue"`
	Units    int64   `json:"units"`
}

type LocationMetric struct {
	Location string  `json:"location"`
	Revenue  float64 `json:"revenue"`
	Orders   int64   `json:"orders"`
}

type TimelinePoint struct {
	Timestamp time.Time `json:"timestamp"`
	Revenue   float64   `json:"revenue"`
	Orders    int64     `json:"orders"`
}

type FraudAlert struct {
	AlertID      string                 `json:"alert_id"`
	AlertType    string                 `json:"alert_type"`
	Severity     string                 `json:"severity"`
	UserID       string                 `json:"user_id"`
	Description  string                 `json:"description"`
	Events       []Event                `json:"events"`
	RiskScore    float64                `json:"risk_score"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata"`
}

type UserActivity struct {
	UserID         string    `json:"user_id"`
	SessionID      string    `json:"session_id"`
	LastSeen       time.Time `json:"last_seen"`
	EventCount     int       `json:"event_count"`
	PurchaseCount  int       `json:"purchase_count"`
	TotalSpent     float64   `json:"total_spent"`
	Locations      []string  `json:"locations"`
	Devices        []string  `json:"devices"`
	SuspiciousFlag bool      `json:"suspicious_flag"`
}

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for demo
	},
}

// Global state
type DemoState struct {
	mu               sync.RWMutex
	salesMetrics     SalesMetrics
	fraudAlerts      []FraudAlert
	userActivities   map[string]UserActivity
	recentEvents     []Event
	websocketClients []*websocket.Conn
	clientMu         sync.RWMutex
}

var demoState = &DemoState{
	salesMetrics:   SalesMetrics{TopProducts: []ProductMetric{}, SalesByCategory: []CategoryMetric{}, SalesByLocation: []LocationMetric{}, RevenueTimeline: []TimelinePoint{}},
	fraudAlerts:    []FraudAlert{},
	userActivities: make(map[string]UserActivity),
	recentEvents:   []Event{},
}

func main() {
	var (
		streamflowHost = flag.String("streamflow-host", "localhost", "StreamFlow host")
		streamflowPort = flag.Int("streamflow-port", 8080, "StreamFlow gRPC port")
		httpPort       = flag.Int("http-port", 8080, "HTTP server port for WebSocket")
		consumerGroup  = flag.String("consumer-group", "demo-analytics", "Consumer group name")
	)
	flag.Parse()

	log.Printf("Starting StreamFlow Demo Analytics Pipeline")
	log.Printf("StreamFlow: %s:%d", *streamflowHost, *streamflowPort)
	log.Printf("HTTP Server: :%d", *httpPort)

	// Create storage connection (for this demo, we'll use in-memory)
	store := storage.NewStorage()

	// Create topic for demo events
	topicName := "demo-events"
	if err := store.CreateTopic(topicName, 4); err != nil {
		log.Printf("Topic might already exist: %v", err)
	}

	// Setup stream processing pipelines
	if err := setupStreamPipelines(store, *consumerGroup, topicName); err != nil {
		log.Fatalf("Failed to setup stream pipelines: %v", err)
	}

	// Start HTTP server for WebSocket connections
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/api/sales", handleMetricsAPI)     // Sales metrics endpoint
	http.HandleFunc("/api/fraud", handleAlertsAPI)      // Fraud alerts endpoint  
	http.HandleFunc("/api/metrics", handleMetricsAPI)   // Legacy endpoint
	http.HandleFunc("/api/alerts", handleAlertsAPI)     // Legacy endpoint
	http.HandleFunc("/health", handleHealthAPI)

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", *httpPort),
	}

	go func() {
		log.Printf("HTTP server starting on port %d", *httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start metrics broadcasting
	go metricsUpdater()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func setupStreamPipelines(store *storage.Storage, consumerGroup, topicName string) error {
	// Sales Analytics Pipeline
	salesConfig := &stream.ProcessorConfig{
		ProcessorName:   "sales-analytics",
		ConsumerGroup:   consumerGroup + "-sales",
		MaxConcurrency:  4,
		BatchSize:       50,
		FlushInterval:   time.Second * 2,
		StateStoreType:  "memory",
	}

	salesProcessor, err := stream.NewStreamProcessor(salesConfig, store)
	if err != nil {
		return fmt.Errorf("failed to create sales processor: %v", err)
	}

	// Fraud Detection Pipeline
	fraudConfig := &stream.ProcessorConfig{
		ProcessorName:   "fraud-detection",
		ConsumerGroup:   consumerGroup + "-fraud",
		MaxConcurrency:  2,
		BatchSize:       25,
		FlushInterval:   time.Second * 1,
		StateStoreType:  "memory",
	}

	fraudProcessor, err := stream.NewStreamProcessor(fraudConfig, store)
	if err != nil {
		return fmt.Errorf("failed to create fraud processor: %v", err)
	}

	// Setup Sales Analytics Stream
	go func() {
		log.Println("Starting sales analytics pipeline...")
		
		salesProcessor.NewStream(topicName).
			Filter(func(event *stream.Event) bool {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return false
				}
				return e.EventType == "purchase"
			}).
			Map(func(event *stream.Event) *stream.Event {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return event
				}
				
				// Process sales event
				processSalesEvent(e)
				
				return event
			}).
			Window(time.Minute * 1).
			GroupBy(func(event *stream.Event) string {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return "unknown"
				}
				if e.ProductCategory != nil {
					return *e.ProductCategory
				}
				return "uncategorized"
			}).
			Sum(func(event *stream.Event) (float64, error) {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return 0, err
				}
				if e.ProductPrice != nil {
					return *e.ProductPrice * float64(e.Quantity), nil
				}
				return 0, nil
			}).
			ForEach(func(result *stream.AggregateResult) {
				log.Printf("Sales by category %s: $%.2f in window [%s - %s]",
					result.Key, result.Sum,
					result.Window.Start.Format("15:04:05"),
					result.Window.End.Format("15:04:05"))
			})
	}()

	// Setup Fraud Detection Stream
	go func() {
		log.Println("Starting fraud detection pipeline...")
		
		fraudProcessor.NewStream(topicName).
			Filter(func(event *stream.Event) bool {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return false
				}
				
				// Store event for fraud analysis
				demoState.mu.Lock()
				demoState.recentEvents = append(demoState.recentEvents, e)
				if len(demoState.recentEvents) > 1000 {
					demoState.recentEvents = demoState.recentEvents[100:]
				}
				demoState.mu.Unlock()
				
				// Track user activity
				updateUserActivity(e)
				
				// Check for suspicious patterns
				return isSuspiciousEvent(e)
			}).
			SessionWindow(time.Minute * 5).  // 5-minute session windows for fraud detection
			GroupBy(func(event *stream.Event) string {
				var e Event
				if err := json.Unmarshal(event.Value, &e); err != nil {
					return "unknown"
				}
				return e.UserID
			}).
			Count().
			ForEach(func(result *stream.AggregateResult) {
				// Generate fraud alert if user has suspicious activity
				if result.Count >= 3 { // 3+ suspicious events in 5 minutes
					generateFraudAlert(result.Key, result.Count, result.Window)
				}
			})
	}()

	return nil
}

func processSalesEvent(event Event) {
	demoState.mu.Lock()
	defer demoState.mu.Unlock()

	// Update revenue
	if event.ProductPrice != nil {
		revenue := *event.ProductPrice * float64(event.Quantity)
		demoState.salesMetrics.TotalRevenue += revenue
		demoState.salesMetrics.TransactionCount++
		demoState.salesMetrics.AverageOrderValue = demoState.salesMetrics.TotalRevenue / float64(demoState.salesMetrics.TransactionCount)

		// Update timeline
		now := time.Now().Truncate(time.Minute)
		if len(demoState.salesMetrics.RevenueTimeline) == 0 || 
			demoState.salesMetrics.RevenueTimeline[len(demoState.salesMetrics.RevenueTimeline)-1].Timestamp.Before(now) {
			demoState.salesMetrics.RevenueTimeline = append(demoState.salesMetrics.RevenueTimeline, TimelinePoint{
				Timestamp: now,
				Revenue:   revenue,
				Orders:    1,
			})
		} else {
			// Update last timeline point
			last := &demoState.salesMetrics.RevenueTimeline[len(demoState.salesMetrics.RevenueTimeline)-1]
			last.Revenue += revenue
			last.Orders++
		}

		// Keep only last 60 minutes of timeline data
		cutoff := time.Now().Add(-time.Hour)
		for i, point := range demoState.salesMetrics.RevenueTimeline {
			if point.Timestamp.After(cutoff) {
				demoState.salesMetrics.RevenueTimeline = demoState.salesMetrics.RevenueTimeline[i:]
				break
			}
		}

		// Update product metrics
		if event.ProductID != nil && event.ProductName != nil {
			updated := false
			for i := range demoState.salesMetrics.TopProducts {
				if demoState.salesMetrics.TopProducts[i].ProductID == *event.ProductID {
					demoState.salesMetrics.TopProducts[i].Revenue += revenue
					demoState.salesMetrics.TopProducts[i].Units += int64(event.Quantity)
					updated = true
					break
				}
			}
			if !updated {
				demoState.salesMetrics.TopProducts = append(demoState.salesMetrics.TopProducts, ProductMetric{
					ProductID:   *event.ProductID,
					ProductName: *event.ProductName,
					Revenue:     revenue,
					Units:       int64(event.Quantity),
				})
			}
		}

		// Update category metrics
		if event.ProductCategory != nil {
			updated := false
			for i := range demoState.salesMetrics.SalesByCategory {
				if demoState.salesMetrics.SalesByCategory[i].Category == *event.ProductCategory {
					demoState.salesMetrics.SalesByCategory[i].Revenue += revenue
					demoState.salesMetrics.SalesByCategory[i].Units += int64(event.Quantity)
					updated = true
					break
				}
			}
			if !updated {
				demoState.salesMetrics.SalesByCategory = append(demoState.salesMetrics.SalesByCategory, CategoryMetric{
					Category: *event.ProductCategory,
					Revenue:  revenue,
					Units:    int64(event.Quantity),
				})
			}
		}

		// Update location metrics
		updated := false
		for i := range demoState.salesMetrics.SalesByLocation {
			if demoState.salesMetrics.SalesByLocation[i].Location == event.Location {
				demoState.salesMetrics.SalesByLocation[i].Revenue += revenue
				demoState.salesMetrics.SalesByLocation[i].Orders++
				updated = true
				break
			}
		}
		if !updated {
			demoState.salesMetrics.SalesByLocation = append(demoState.salesMetrics.SalesByLocation, LocationMetric{
				Location: event.Location,
				Revenue:  revenue,
				Orders:   1,
			})
		}
	}

	demoState.salesMetrics.LastUpdated = time.Now()
}

func updateUserActivity(event Event) {
	demoState.mu.Lock()
	defer demoState.mu.Unlock()

	activity := demoState.userActivities[event.UserID]
	activity.UserID = event.UserID
	activity.SessionID = event.SessionID
	activity.LastSeen = event.Timestamp
	activity.EventCount++

	if event.EventType == "purchase" && event.ProductPrice != nil {
		activity.PurchaseCount++
		activity.TotalSpent += *event.ProductPrice * float64(event.Quantity)
	}

	// Track locations
	found := false
	for _, loc := range activity.Locations {
		if loc == event.Location {
			found = true
			break
		}
	}
	if !found {
		activity.Locations = append(activity.Locations, event.Location)
	}

	// Track devices
	found = false
	for _, device := range activity.Devices {
		if device == event.DeviceType {
			found = true
			break
		}
	}
	if !found {
		activity.Devices = append(activity.Devices, event.DeviceType)
	}

	// Mark as suspicious if metadata indicates so
	if event.Metadata != nil {
		if suspicious, ok := event.Metadata["suspicious"].(bool); ok && suspicious {
			activity.SuspiciousFlag = true
		}
	}

	demoState.userActivities[event.UserID] = activity
}

func isSuspiciousEvent(event Event) bool {
	// Check metadata for suspicious flag
	if event.Metadata != nil {
		if suspicious, ok := event.Metadata["suspicious"].(bool); ok && suspicious {
			return true
		}
	}

	// Check for suspicious patterns
	if event.EventType == "purchase" {
		// Large quantity purchases
		if event.Quantity > 10 {
			return true
		}

		// High-value purchases
		if event.ProductPrice != nil && *event.ProductPrice > 1000 {
			return true
		}
	}

	// Multiple locations for same user
	demoState.mu.RLock()
	if activity, exists := demoState.userActivities[event.UserID]; exists {
		if len(activity.Locations) > 2 {
			demoState.mu.RUnlock()
			return true
		}
	}
	demoState.mu.RUnlock()

	// Suspicious locations
	suspiciousLocations := []string{"UNKNOWN", "VPN", "PROXY"}
	for _, loc := range suspiciousLocations {
		if event.Location == loc {
			return true
		}
	}

	return false
}

func generateFraudAlert(userID string, eventCount int64, window *stream.Window) {
	demoState.mu.Lock()
	defer demoState.mu.Unlock()

	alert := FraudAlert{
		AlertID:     fmt.Sprintf("alert-%d", time.Now().Unix()),
		AlertType:   "suspicious_behavior",
		Severity:    "medium",
		UserID:      userID,
		Description: fmt.Sprintf("User %s had %d suspicious events in 5-minute window", userID, eventCount),
		RiskScore:   float64(eventCount) * 0.3,
		Timestamp:   time.Now(),
	}

	if eventCount > 10 {
		alert.Severity = "high"
		alert.RiskScore = 0.9
	} else if eventCount > 5 {
		alert.Severity = "medium"
		alert.RiskScore = 0.6
	}

	// Find related events
	for _, event := range demoState.recentEvents {
		if event.UserID == userID && 
		   event.Timestamp.After(window.Start) && 
		   event.Timestamp.Before(window.End) {
			alert.Events = append(alert.Events, event)
		}
	}

	demoState.fraudAlerts = append(demoState.fraudAlerts, alert)
	
	// Keep only last 100 alerts
	if len(demoState.fraudAlerts) > 100 {
		demoState.fraudAlerts = demoState.fraudAlerts[len(demoState.fraudAlerts)-100:]
	}

	log.Printf("🚨 FRAUD ALERT: %s - %s (Risk: %.1f)", alert.AlertID, alert.Description, alert.RiskScore)
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	demoState.clientMu.Lock()
	demoState.websocketClients = append(demoState.websocketClients, conn)
	demoState.clientMu.Unlock()

	log.Printf("WebSocket client connected. Total clients: %d", len(demoState.websocketClients))

	// Send initial data
	demoState.mu.RLock()
	initialData := map[string]interface{}{
		"type":         "initial_data",
		"metrics":      demoState.salesMetrics,
		"alerts":       demoState.fraudAlerts,
		"user_count":   len(demoState.userActivities),
	}
	demoState.mu.RUnlock()

	conn.WriteJSON(initialData)

	// Keep connection alive and handle disconnection
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			// Remove client from list
			demoState.clientMu.Lock()
			for i, client := range demoState.websocketClients {
				if client == conn {
					demoState.websocketClients = append(demoState.websocketClients[:i], demoState.websocketClients[i+1:]...)
					break
				}
			}
			demoState.clientMu.Unlock()
			log.Printf("WebSocket client disconnected. Remaining clients: %d", len(demoState.websocketClients))
			break
		}
	}
}

func metricsUpdater() {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	for range ticker.C {
		demoState.clientMu.RLock()
		clients := make([]*websocket.Conn, len(demoState.websocketClients))
		copy(clients, demoState.websocketClients)
		demoState.clientMu.RUnlock()

		if len(clients) == 0 {
			continue
		}

		demoState.mu.RLock()
		update := map[string]interface{}{
			"type":       "update",
			"metrics":    demoState.salesMetrics,
			"alerts":     demoState.fraudAlerts[max(0, len(demoState.fraudAlerts)-10):], // Last 10 alerts
			"user_count": len(demoState.userActivities),
			"timestamp":  time.Now(),
		}
		demoState.mu.RUnlock()

		// Broadcast to all clients
		for _, client := range clients {
			if err := client.WriteJSON(update); err != nil {
				log.Printf("Error sending WebSocket update: %v", err)
			}
		}
	}
}

func handleMetricsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	demoState.mu.RLock()
	json.NewEncoder(w).Encode(demoState.salesMetrics)
	demoState.mu.RUnlock()
}

func handleAlertsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	demoState.mu.RLock()
	json.NewEncoder(w).Encode(demoState.fraudAlerts)
	demoState.mu.RUnlock()
}

func handleHealthAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	health := map[string]interface{}{
		"status":           "healthy",
		"websocket_clients": len(demoState.websocketClients),
		"total_revenue":    demoState.salesMetrics.TotalRevenue,
		"total_alerts":     len(demoState.fraudAlerts),
		"active_users":     len(demoState.userActivities),
	}
	
	json.NewEncoder(w).Encode(health)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}