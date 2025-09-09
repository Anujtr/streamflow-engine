"""
Event generator for creating realistic e-commerce events
"""
import asyncio
import random
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Set
from models import (
    User, Product, EcommerceEvent, EventType, TrafficPattern,
    SAMPLE_PRODUCTS, SAMPLE_SEARCH_QUERIES, SAMPLE_LOCATIONS, 
    SAMPLE_USER_AGENTS, TRAFFIC_PATTERNS
)


class UserSession:
    """Tracks user session state for realistic behavior"""
    
    def __init__(self, user: User):
        self.user = user
        self.session_start = datetime.now(timezone.utc)
        self.last_activity = self.session_start
        self.pages_viewed = 0
        self.products_viewed: Set[str] = set()
        self.cart_items: Dict[str, int] = {}  # product_id -> quantity
        self.search_history: List[str] = []
        self.is_fraudulent = False
        self.device_type = random.choice(["desktop", "mobile", "tablet"])
        
    def is_active(self, max_idle_minutes: int = 30) -> bool:
        """Check if session is still active"""
        idle_time = datetime.now(timezone.utc) - self.last_activity
        return idle_time.total_seconds() < (max_idle_minutes * 60)
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now(timezone.utc)


class EventGenerator:
    """Generates realistic e-commerce events"""
    
    def __init__(self, streamflow_host: str = "localhost", streamflow_port: int = 8080):
        self.streamflow_host = streamflow_host
        self.streamflow_port = streamflow_port
        self.active_sessions: Dict[str, UserSession] = {}
        self.products = SAMPLE_PRODUCTS.copy()
        self.event_history: List[EcommerceEvent] = []
        self.running = False
        self.stats = {
            "total_events": 0,
            "events_by_type": {event_type: 0 for event_type in EventType},
            "unique_users": 0,
            "fraudulent_events": 0
        }
        
    async def generate_user(self, is_fraudulent: bool = False) -> User:
        """Generate a new user"""
        user_id = f"user_{random.randint(10000, 99999)}"
        
        if is_fraudulent:
            # Fraudulent users have suspicious patterns
            email = f"fake_{random.randint(1000, 9999)}@temp.com"
            name = f"FakeUser{random.randint(100, 999)}"
            location = random.choice(["UNKNOWN", "VPN", "PROXY"])
        else:
            # Regular users
            email = f"{random.choice(['john', 'jane', 'bob', 'alice', 'charlie', 'diana'])}.{random.choice(['smith', 'doe', 'johnson', 'williams', 'brown'])}@{random.choice(['gmail.com', 'yahoo.com', 'hotmail.com'])}"
            name = email.split('@')[0].replace('.', ' ').title()
            location = random.choice(SAMPLE_LOCATIONS)
        
        user = User(
            user_id=user_id,
            email=email,
            name=name,
            location=location,
            registration_date=datetime.now(timezone.utc) - timedelta(days=random.randint(1, 365)),
            is_premium=random.random() < 0.1  # 10% premium users
        )
        
        session = UserSession(user)
        session.is_fraudulent = is_fraudulent
        self.active_sessions[user.session_id] = session
        
        return user
    
    def cleanup_inactive_sessions(self):
        """Remove inactive sessions"""
        current_time = datetime.now(timezone.utc)
        inactive_sessions = [
            session_id for session_id, session in self.active_sessions.items()
            if not session.is_active()
        ]
        
        for session_id in inactive_sessions:
            del self.active_sessions[session_id]
    
    async def generate_event(self, pattern: TrafficPattern) -> Optional[EcommerceEvent]:
        """Generate a single event based on traffic pattern"""
        
        # Determine event type based on distribution
        event_type = random.choices(
            list(pattern.event_distribution.keys()),
            weights=list(pattern.event_distribution.values())
        )[0]
        
        # Determine if this should be a fraudulent event
        is_fraud = random.random() < pattern.fraud_probability
        
        # Get or create user session
        if not self.active_sessions or random.random() < 0.1:  # 10% chance of new session
            user = await self.generate_user(is_fraudulent=is_fraud)
            session = self.active_sessions[user.session_id]
        else:
            session = random.choice(list(self.active_sessions.values()))
            user = session.user
        
        session.update_activity()
        
        # Generate event based on type
        event = await self._create_event_by_type(event_type, user, session, is_fraud)
        
        if event:
            self.event_history.append(event)
            self.stats["total_events"] += 1
            self.stats["events_by_type"][event_type] += 1
            if is_fraud:
                self.stats["fraudulent_events"] += 1
        
        return event
    
    async def _create_event_by_type(self, event_type: EventType, user: User, session: UserSession, is_fraud: bool) -> Optional[EcommerceEvent]:
        """Create specific event type"""
        
        base_event_data = {
            "event_type": event_type,
            "user_id": user.user_id,
            "session_id": session.user.session_id,
            "location": user.location,
            "device_type": session.device_type,
            "user_agent": random.choice(SAMPLE_USER_AGENTS)
        }
        
        if is_fraud:
            base_event_data["metadata"] = {"suspicious": True, "fraud_score": random.uniform(0.7, 1.0)}
        
        if event_type == EventType.PAGE_VIEW:
            return EcommerceEvent(
                **base_event_data,
                page_url=f"/page/{random.choice(['home', 'products', 'categories', 'about', 'contact'])}",
                referrer=random.choice([None, "google.com", "facebook.com", "direct"])
            )
        
        elif event_type == EventType.PRODUCT_VIEW:
            product = random.choice(self.products)
            session.products_viewed.add(product.product_id)
            session.pages_viewed += 1
            
            return EcommerceEvent(
                **base_event_data,
                product_id=product.product_id,
                product_name=product.name,
                product_category=product.category,
                product_price=product.price,
                page_url=f"/product/{product.product_id}"
            )
        
        elif event_type == EventType.ADD_TO_CART:
            if session.products_viewed:
                product_id = random.choice(list(session.products_viewed))
                product = next(p for p in self.products if p.product_id == product_id)
                quantity = random.randint(1, 3) if not is_fraud else random.randint(5, 20)  # Fraud: large quantities
                
                session.cart_items[product_id] = session.cart_items.get(product_id, 0) + quantity
                
                return EcommerceEvent(
                    **base_event_data,
                    product_id=product.product_id,
                    product_name=product.name,
                    product_category=product.category,
                    product_price=product.price,
                    quantity=quantity
                )
        
        elif event_type == EventType.PURCHASE:
            if session.cart_items:
                # Purchase items in cart
                total_amount = 0
                purchased_items = []
                
                for product_id, quantity in session.cart_items.items():
                    product = next(p for p in self.products if p.product_id == product_id)
                    item_total = product.price * quantity
                    total_amount += item_total
                    purchased_items.append({
                        "product_id": product_id,
                        "quantity": quantity,
                        "price": product.price,
                        "total": item_total
                    })
                
                # Fraud: unusually high amounts or rapid purchases
                if is_fraud:
                    total_amount *= random.uniform(5, 20)  # Inflate amount
                
                session.cart_items.clear()  # Empty cart after purchase
                
                return EcommerceEvent(
                    **base_event_data,
                    quantity=len(purchased_items),
                    metadata={
                        "total_amount": total_amount,
                        "items": purchased_items,
                        "payment_method": "credit_card" if not is_fraud else random.choice(["stolen_card", "fake_account"]),
                        **(base_event_data.get("metadata", {}))
                    }
                )
        
        elif event_type == EventType.SEARCH:
            query = random.choice(SAMPLE_SEARCH_QUERIES)
            session.search_history.append(query)
            results_count = random.randint(5, 50) if not is_fraud else random.randint(0, 2)  # Fraud: poor search results
            
            return EcommerceEvent(
                **base_event_data,
                search_query=query,
                search_results_count=results_count
            )
        
        elif event_type == EventType.USER_LOGIN:
            # Fraud: multiple rapid logins, suspicious locations
            if is_fraud:
                base_event_data["metadata"] = {
                    "login_attempts": random.randint(5, 20),
                    "suspicious_ip": True,
                    "location_mismatch": True,
                    **(base_event_data.get("metadata", {}))
                }
            
            return EcommerceEvent(**base_event_data)
        
        elif event_type == EventType.USER_REGISTRATION:
            return EcommerceEvent(
                **base_event_data,
                metadata={
                    "registration_source": random.choice(["organic", "referral", "advertisement"]),
                    "email_verified": not is_fraud,  # Fraud: unverified emails
                    **(base_event_data.get("metadata", {}))
                }
            )
        
        return None
    
    async def run_traffic_pattern(self, pattern: TrafficPattern, websocket_broadcaster=None):
        """Run a specific traffic pattern"""
        print(f"Starting traffic pattern: {pattern.name}")
        print(f"Target: {pattern.events_per_second} events/sec for {pattern.duration_seconds} seconds")
        
        start_time = time.time()
        end_time = start_time + pattern.duration_seconds
        events_generated = 0
        
        self.running = True
        
        while time.time() < end_time and self.running:
            batch_start = time.time()
            
            # Calculate events for this batch (1 second intervals)
            target_events = int(pattern.events_per_second * pattern.burst_multiplier)
            
            # Generate events in batches to avoid overwhelming
            for _ in range(target_events):
                if not self.running:
                    break
                    
                event = await self.generate_event(pattern)
                if event and websocket_broadcaster:
                    await websocket_broadcaster.broadcast_event(event)
                
                events_generated += 1
                
                # Small delay to control rate
                if events_generated % 10 == 0:
                    await asyncio.sleep(0.01)
            
            # Cleanup sessions periodically
            if events_generated % 100 == 0:
                self.cleanup_inactive_sessions()
            
            # Wait for next batch
            batch_duration = time.time() - batch_start
            if batch_duration < 1.0:
                await asyncio.sleep(1.0 - batch_duration)
        
        print(f"Completed traffic pattern: {pattern.name}")
        print(f"Generated {events_generated} events")
        self.stats["unique_users"] = len(self.active_sessions)
    
    def stop(self):
        """Stop event generation"""
        self.running = False
    
    def get_statistics(self) -> Dict:
        """Get current statistics"""
        return {
            **self.stats,
            "active_sessions": len(self.active_sessions),
            "patterns_available": list(TRAFFIC_PATTERNS.keys())
        }
    
    async def get_recent_events(self, limit: int = 100) -> List[Dict]:
        """Get recent events for dashboard"""
        recent = self.event_history[-limit:] if self.event_history else []
        return [event.to_streamflow_message() for event in recent]