"""
StreamFlow gRPC client for sending events
"""
import grpc
import asyncio
from typing import Dict, Any, Optional
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the generated protobuf classes
try:
    import streamflow_pb2
    import streamflow_pb2_grpc
    from google.protobuf.timestamp_pb2 import Timestamp
    PROTOBUF_AVAILABLE = True
    logger.info("StreamFlow protobuf files loaded successfully")
except ImportError as e:
    logger.warning(f"StreamFlow protobuf files not found: {e}. Using mock client.")
    streamflow_pb2 = None
    streamflow_pb2_grpc = None
    Timestamp = None
    PROTOBUF_AVAILABLE = False


class StreamFlowClient:
    """Async gRPC client for StreamFlow Engine"""
    
    def __init__(self, host: str = "localhost", port: int = 9090):  # gRPC port
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
                logger.info("Using mock StreamFlow client (protobuf not available)")
                self.connected = True
                return True
            
            logger.info(f"Connecting to StreamFlow Engine at {self.host}:{self.port}")
            self.channel = grpc.aio.insecure_channel(f'{self.host}:{self.port}')
            self.stub = streamflow_pb2_grpc.MessageServiceStub(self.channel)
            
            # Test connection with a simple health check
            health_request = streamflow_pb2.HealthRequest()
            health_response = await asyncio.wait_for(
                self.stub.Health(health_request), 
                timeout=5.0
            )
            logger.info(f"Connected to StreamFlow Engine: {health_response.status}")
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to StreamFlow Engine: {e}")
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
            self.connected = False
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from StreamFlow Engine"""
        if self.channel:
            await self.channel.close()
        self.connected = False
        logger.info("Disconnected from StreamFlow Engine")
    
    async def send_event(self, topic: str, event_data: Dict[str, Any]) -> bool:
        """Send a single event to StreamFlow"""
        if not self.connected:
            if not await self.connect():
                return False
        
        try:
            if not PROTOBUF_AVAILABLE:
                # Mock implementation for development
                await asyncio.sleep(0.001)  # Simulate network delay
                self.stats["messages_sent"] += 1
                logger.debug(f"Mock sent event to topic {topic}")
                return True
            
            # Prepare message content
            key = event_data.get("event_id", event_data.get("user_id", ""))
            value = json.dumps(event_data).encode('utf-8')
            
            # Create timestamp
            timestamp = Timestamp()
            if "timestamp" in event_data:
                if isinstance(event_data["timestamp"], str):
                    dt = datetime.fromisoformat(event_data["timestamp"].replace('Z', '+00:00'))
                elif isinstance(event_data["timestamp"], datetime):
                    dt = event_data["timestamp"]
                else:
                    dt = datetime.now()
            else:
                dt = datetime.now()
            timestamp.FromDatetime(dt)
            
            # Create protobuf message
            message = streamflow_pb2.Message(
                key=key,
                value=value,
                timestamp=timestamp
            )
            
            # Create produce request
            request = streamflow_pb2.ProduceRequest(
                topic=topic,
                messages=[message]
            )
            
            # Send message to StreamFlow
            response = await asyncio.wait_for(
                self.stub.Produce(request), 
                timeout=10.0
            )
            
            # Check for errors in response
            if response.results and response.results[0].error:
                raise Exception(f"StreamFlow error: {response.results[0].error}")
            
            self.stats["messages_sent"] += 1
            logger.debug(f"Sent event to StreamFlow topic {topic}: {key}")
            return True
            
        except Exception as e:
            self.stats["errors"] += 1
            self.stats["last_error"] = str(e)
            logger.error(f"Error sending event: {e}")
            return False
    
    async def send_events_batch(self, topic: str, events: list[Dict[str, Any]]) -> int:
        """Send multiple events in a batch"""
        if not self.connected:
            if not await self.connect():
                return 0
        
        successful = 0
        for event in events:
            if await self.send_event(topic, event):
                successful += 1
        
        return successful
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics"""
        return self.stats.copy()
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()