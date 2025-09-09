"""
StreamFlow gRPC client for sending events
"""
import grpc
import asyncio
from typing import Dict, Any, Optional
import json
from datetime import datetime
import logging

# Import the generated protobuf classes
try:
    import streamflow_pb2
    import streamflow_pb2_grpc
    from google.protobuf.timestamp_pb2 import Timestamp
    PROTOBUF_AVAILABLE = True
except ImportError as e:
    logging.warning(f"StreamFlow protobuf files not found: {e}. Using mock client.")
    streamflow_pb2 = None
    streamflow_pb2_grpc = None
    Timestamp = None
    PROTOBUF_AVAILABLE = False


class StreamFlowClient:
    """Async gRPC client for StreamFlow Engine"""
    
    def __init__(self, host: str = "localhost", port: int = 9090):  # Changed to gRPC port
        self.host = host
        self.port = port
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub = None
        self.connected = False
        self.stats = {
            "messages_sent": 0,
            "errors": 0,
            "last_error": None
        }
    
    async def connect(self) -> bool:
        """Connect to StreamFlow Engine"""
        try:
            if not PROTOBUF_AVAILABLE:
                logging.info("Using mock StreamFlow client (protobuf not available)")
                self.connected = True
                return True
            
            self.channel = grpc.aio.insecure_channel(f'{self.host}:{self.port}')
            self.stub = streamflow_pb2_grpc.MessageServiceStub(self.channel)
            
            # Test connection with a simple health check
            health_request = streamflow_pb2.HealthRequest()
            health_response = await self.stub.Health(health_request, timeout=5.0)
            logging.info(f"Connected to StreamFlow Engine: {health_response.status}")
            self.connected = True
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to StreamFlow Engine: {e}")
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
            self.connected = False
            return False
    
    async def _health_check(self):
        """Simple health check by attempting to list topics"""
        if streamflow_pb2 is None:
            return  # Mock client always healthy
        
        # This is a basic check - in real implementation, you might want a dedicated health endpoint
        pass  # For now, just connection attempt is the health check
    
    async def send_event(self, topic: str, event_data: Dict[str, Any]) -> bool:
        """Send a single event to StreamFlow"""
        if not self.connected:
            if not await self.connect():
                return False
        
        try:
            if streamflow_pb2 is None:
                # Mock implementation
                await asyncio.sleep(0.001)  # Simulate network delay
                self.stats["messages_sent"] += 1
                return True
            
            # Create protobuf message
            message = streamflow_pb2.Message(
                key=event_data.get("key", ""),
                value=event_data.get("value", "").encode('utf-8') if isinstance(event_data.get("value"), str) else str(event_data.get("value")).encode('utf-8'),
                timestamp=streamflow_pb2.google_dot_protobuf_dot_timestamp__pb2.Timestamp()
            )
            
            # Set timestamp if provided
            if "timestamp" in event_data:
                if isinstance(event_data["timestamp"], str):
                    dt = datetime.fromisoformat(event_data["timestamp"].replace('Z', '+00:00'))
                elif isinstance(event_data["timestamp"], datetime):
                    dt = event_data["timestamp"]
                else:
                    dt = datetime.now()
                message.timestamp.FromDatetime(dt)
            
            # Create produce request
            request = streamflow_pb2.ProduceRequest(
                topic=topic,
                messages=[message]
            )
            
            # Send message
            response = await self.stub.Produce(request)
            
            # Check for errors in response
            if response.results and response.results[0].error:
                raise Exception(f"StreamFlow error: {response.results[0].error}")
            
            self.stats["messages_sent"] += 1
            return True
            
        except Exception as e:
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
            print(f"Error sending event: {e}")
            
            # Try to reconnect on error
            self.connected = False
            return False
    
    async def send_events_batch(self, topic: str, events: list[Dict[str, Any]]) -> int:
        """Send multiple events in a batch"""
        if not self.connected:
            if not await self.connect():
                return 0
        
        if streamflow_pb2 is None:
            # Mock implementation
            await asyncio.sleep(0.01)  # Simulate batch processing delay
            self.stats["messages_sent"] += len(events)
            return len(events)
        
        try:
            # Create protobuf messages
            messages = []
            for event_data in events:
                message = streamflow_pb2.Message(
                    key=event_data.get("key", ""),
                    value=event_data.get("value", "").encode('utf-8') if isinstance(event_data.get("value"), str) else str(event_data.get("value")).encode('utf-8'),
                    timestamp=streamflow_pb2.google_dot_protobuf_dot_timestamp__pb2.Timestamp()
                )
                
                # Set timestamp if provided
                if "timestamp" in event_data:
                    if isinstance(event_data["timestamp"], str):
                        dt = datetime.fromisoformat(event_data["timestamp"].replace('Z', '+00:00'))
                    elif isinstance(event_data["timestamp"], datetime):
                        dt = event_data["timestamp"]
                    else:
                        dt = datetime.now()
                    message.timestamp.FromDatetime(dt)
                
                messages.append(message)
            
            # Create batch produce request
            request = streamflow_pb2.ProduceRequest(
                topic=topic,
                messages=messages
            )
            
            # Send batch
            response = await self.stub.Produce(request)
            
            # Count successful sends
            successful = 0
            for result in response.results:
                if not result.error:
                    successful += 1
                else:
                    self.stats["errors"] += 1
                    print(f"Batch error: {result.error}")
            
            self.stats["messages_sent"] += successful
            return successful
            
        except Exception as e:
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
            print(f"Error sending batch: {e}")
            
            # Try to reconnect on error
            self.connected = False
            return 0
    
    async def disconnect(self):
        """Disconnect from StreamFlow Engine"""
        if self.channel:
            await self.channel.close()
        self.connected = False
        print("Disconnected from StreamFlow Engine")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        return {
            "connected": self.connected,
            "host": self.host,
            "port": self.port,
            **self.stats
        }