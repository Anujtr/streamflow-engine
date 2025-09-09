"""
FastAPI application for StreamFlow Engine demo simulator
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uvicorn

from models import TrafficPattern, TRAFFIC_PATTERNS
from event_generator import EventGenerator
from streamflow_client import StreamFlowClient


class WebSocketBroadcaster:
    """Manages WebSocket connections and broadcasts"""
    
    def __init__(self):
        self.connections: List[WebSocket] = []
        self.event_buffer: List[Dict] = []
        self.buffer_size = 1000
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)
        print(f"WebSocket connected. Total connections: {len(self.connections)}")
        
        # Send recent events to new connection
        if self.event_buffer:
            await websocket.send_json({
                "type": "event_history",
                "data": self.event_buffer[-100:]  # Last 100 events
            })
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.connections:
            self.connections.remove(websocket)
        print(f"WebSocket disconnected. Total connections: {len(self.connections)}")
    
    async def broadcast_event(self, event):
        """Broadcast event to all connected clients"""
        if not self.connections:
            return
        
        message = {
            "type": "event",
            "data": event.to_streamflow_message(),
            "timestamp": datetime.now().isoformat()
        }
        
        # Add to buffer
        self.event_buffer.append(message)
        if len(self.event_buffer) > self.buffer_size:
            self.event_buffer.pop(0)
        
        # Broadcast to all connections
        disconnected = []
        for websocket in self.connections:
            try:
                await websocket.send_json(message)
            except Exception:
                disconnected.append(websocket)
        
        # Remove disconnected clients
        for ws in disconnected:
            self.disconnect(ws)
    
    async def broadcast_stats(self, stats: Dict):
        """Broadcast statistics to all connected clients"""
        if not self.connections:
            return
        
        message = {
            "type": "stats",
            "data": stats,
            "timestamp": datetime.now().isoformat()
        }
        
        disconnected = []
        for websocket in self.connections:
            try:
                await websocket.send_json(message)
            except Exception:
                disconnected.append(websocket)
        
        for ws in disconnected:
            self.disconnect(ws)


# Global instances
event_generator = EventGenerator()
streamflow_client = StreamFlowClient()
websocket_broadcaster = WebSocketBroadcaster()
current_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting StreamFlow Demo Simulator")
    await streamflow_client.connect()
    yield
    # Shutdown
    print("Shutting down StreamFlow Demo Simulator")
    if current_task:
        current_task.cancel()
    await streamflow_client.disconnect()


# Create FastAPI app
app = FastAPI(
    title="StreamFlow Engine Demo Simulator",
    description="E-commerce event simulator for StreamFlow Engine demonstration",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamFlow Demo Simulator</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .status { padding: 20px; background: #f0f0f0; border-radius: 5px; }
            .controls { margin: 20px 0; }
            .button { 
                padding: 10px 20px; 
                margin: 5px; 
                background: #007bff; 
                color: white; 
                border: none; 
                border-radius: 3px; 
                cursor: pointer; 
            }
            .button:hover { background: #0056b3; }
            .log { 
                height: 300px; 
                overflow-y: scroll; 
                border: 1px solid #ccc; 
                padding: 10px; 
                background: #f8f9fa; 
                font-family: monospace; 
            }
        </style>
    </head>
    <body>
        <h1>StreamFlow Engine Demo Simulator</h1>
        <div class="status" id="status">Connecting...</div>
        
        <div class="controls">
            <h3>Traffic Patterns</h3>
            <button class="button" onclick="startPattern('normal')">Normal Traffic</button>
            <button class="button" onclick="startPattern('flash_sale')">Flash Sale</button>
            <button class="button" onclick="startPattern('fraud_attack')">Fraud Attack</button>
            <button class="button" onclick="startPattern('peak_hours')">Peak Hours</button>
            <button class="button" onclick="stopPattern()">Stop</button>
        </div>
        
        <h3>Live Event Stream</h3>
        <div class="log" id="eventLog"></div>
        
        <script>
            const ws = new WebSocket('ws://localhost:8000/ws');
            const eventLog = document.getElementById('eventLog');
            const status = document.getElementById('status');
            
            ws.onopen = function() {
                status.innerHTML = '<span style="color: green;">✅ Connected to simulator</span>';
            };
            
            ws.onmessage = function(event) {
                const message = JSON.parse(event.data);
                
                if (message.type === 'event') {
                    const eventData = message.data;
                    const logEntry = `${message.timestamp}: ${eventData.headers?.event_type} - ${eventData.key}\\n`;
                    eventLog.innerHTML += logEntry;
                    eventLog.scrollTop = eventLog.scrollHeight;
                } else if (message.type === 'stats') {
                    status.innerHTML = `
                        <span style="color: green;">✅ Connected</span> | 
                        Events: ${message.data.total_events} | 
                        Users: ${message.data.unique_users} | 
                        Fraud: ${message.data.fraudulent_events}
                    `;
                }
            };
            
            ws.onerror = function() {
                status.innerHTML = '<span style="color: red;">❌ Connection error</span>';
            };
            
            async function startPattern(pattern) {
                try {
                    const response = await fetch(`/start-pattern/${pattern}`, { method: 'POST' });
                    const result = await response.json();
                    console.log(result);
                } catch (error) {
                    console.error('Error starting pattern:', error);
                }
            }
            
            async function stopPattern() {
                try {
                    const response = await fetch('/stop-pattern', { method: 'POST' });
                    const result = await response.json();
                    console.log(result);
                } catch (error) {
                    console.error('Error stopping pattern:', error);
                }
            }
        </script>
    </body>
    </html>
    """)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket_broadcaster.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_broadcaster.disconnect(websocket)


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "simulator": "running",
        "streamflow_connected": streamflow_client.connected,
        "websocket_connections": len(websocket_broadcaster.connections),
        "patterns_available": list(TRAFFIC_PATTERNS.keys())
    }


@app.get("/stats")
async def get_stats():
    return {
        "generator_stats": event_generator.get_statistics(),
        "client_stats": streamflow_client.get_stats(),
        "websocket_connections": len(websocket_broadcaster.connections)
    }


@app.get("/patterns")
async def get_patterns():
    return {pattern_name: pattern.model_dump() for pattern_name, pattern in TRAFFIC_PATTERNS.items()}


@app.post("/start-pattern/{pattern_name}")
async def start_pattern(pattern_name: str):
    global current_task
    
    if pattern_name not in TRAFFIC_PATTERNS:
        raise HTTPException(status_code=404, detail=f"Pattern '{pattern_name}' not found")
    
    if current_task and not current_task.done():
        current_task.cancel()
        await asyncio.sleep(0.1)  # Let it cancel
    
    pattern = TRAFFIC_PATTERNS[pattern_name]
    
    async def run_pattern_with_streamflow():
        """Run pattern and send events to StreamFlow"""
        event_batch = []
        batch_size = 10
        
        # Override the event generator to send to StreamFlow
        original_generate_event = event_generator.generate_event
        
        async def generate_and_send_event(pattern):
            event = await original_generate_event(pattern)
            if event:
                # Send to StreamFlow
                success = await streamflow_client.send_event("demo-events", event.to_streamflow_message())
                if not success:
                    print(f"Failed to send event to StreamFlow: {event.event_id}")
                
                # Broadcast to WebSocket
                await websocket_broadcaster.broadcast_event(event)
            
            return event
        
        # Monkey patch for this pattern run
        event_generator.generate_event = generate_and_send_event
        
        try:
            await event_generator.run_traffic_pattern(pattern, websocket_broadcaster)
        finally:
            # Restore original method
            event_generator.generate_event = original_generate_event
    
    # Start stats broadcasting
    async def broadcast_stats():
        while not current_task.done():
            stats = event_generator.get_statistics()
            await websocket_broadcaster.broadcast_stats(stats)
            await asyncio.sleep(2)  # Update every 2 seconds
    
    # Run both tasks concurrently
    current_task = asyncio.create_task(asyncio.gather(
        run_pattern_with_streamflow(),
        broadcast_stats()
    ))
    
    return {
        "status": "started",
        "pattern": pattern_name,
        "description": pattern.description,
        "duration": f"{pattern.duration_seconds} seconds"
    }


@app.post("/stop-pattern")
async def stop_pattern():
    global current_task
    
    if current_task and not current_task.done():
        event_generator.stop()
        current_task.cancel()
        try:
            await asyncio.wait_for(current_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        current_task = None
        return {"status": "stopped"}
    
    return {"status": "no_pattern_running"}


@app.get("/events/recent")
async def get_recent_events(limit: int = 100):
    return await event_generator.get_recent_events(limit)


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True
    )