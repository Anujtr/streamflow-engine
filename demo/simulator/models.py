"""
E-commerce event models for StreamFlow Engine demo
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
import uuid
import random


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    SEARCH = "search"
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    USER_REGISTRATION = "user_registration"


class User(BaseModel):
    user_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    email: str
    name: str
    location: str
    registration_date: datetime
    is_premium: bool = False
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:12])


class Product(BaseModel):
    product_id: str
    name: str
    category: str
    price: float
    inventory_count: int = 1000
    brand: str


class EcommerceEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    user_id: str
    session_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Event-specific data
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    product_category: Optional[str] = None
    product_price: Optional[float] = None
    quantity: int = 1
    
    # Search-specific
    search_query: Optional[str] = None
    search_results_count: Optional[int] = None
    
    # User behavior
    page_url: Optional[str] = None
    referrer: Optional[str] = None
    user_agent: Optional[str] = None
    
    # Geographic and device info
    location: str = "US"
    device_type: str = "desktop"
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_streamflow_message(self) -> Dict[str, Any]:
        """Convert to StreamFlow message format"""
        return {
            "key": f"{self.event_type}:{self.user_id}:{self.event_id[:8]}",
            "value": self.model_dump_json(),
            "timestamp": self.timestamp.isoformat(),
            "headers": {
                "event_type": self.event_type,
                "user_id": self.user_id,
                "session_id": self.session_id
            }
        }


class TrafficPattern(BaseModel):
    name: str
    description: str
    events_per_second: int
    duration_seconds: int
    event_distribution: Dict[EventType, float]  # Probability weights
    burst_multiplier: float = 1.0
    fraud_probability: float = 0.0  # 0.0 = no fraud, 1.0 = all fraud


# Predefined traffic patterns
TRAFFIC_PATTERNS = {
    "normal": TrafficPattern(
        name="normal",
        description="Regular e-commerce traffic",
        events_per_second=100,
        duration_seconds=300,
        event_distribution={
            EventType.PAGE_VIEW: 0.4,
            EventType.PRODUCT_VIEW: 0.25,
            EventType.ADD_TO_CART: 0.15,
            EventType.SEARCH: 0.1,
            EventType.PURCHASE: 0.05,
            EventType.USER_LOGIN: 0.03,
            EventType.USER_REGISTRATION: 0.02
        },
        fraud_probability=0.001
    ),
    
    "flash_sale": TrafficPattern(
        name="flash_sale",
        description="Flash sale event with traffic spike",
        events_per_second=500,
        duration_seconds=180,
        event_distribution={
            EventType.PAGE_VIEW: 0.3,
            EventType.PRODUCT_VIEW: 0.35,
            EventType.ADD_TO_CART: 0.25,
            EventType.PURCHASE: 0.08,
            EventType.SEARCH: 0.02
        },
        burst_multiplier=3.0,
        fraud_probability=0.002
    ),
    
    "fraud_attack": TrafficPattern(
        name="fraud_attack",
        description="Simulated fraud attack",
        events_per_second=200,
        duration_seconds=120,
        event_distribution={
            EventType.USER_LOGIN: 0.3,
            EventType.PRODUCT_VIEW: 0.25,
            EventType.ADD_TO_CART: 0.2,
            EventType.PURCHASE: 0.25
        },
        fraud_probability=0.6  # 60% of events are fraudulent
    ),
    
    "peak_hours": TrafficPattern(
        name="peak_hours", 
        description="Peak shopping hours",
        events_per_second=300,
        duration_seconds=600,
        event_distribution={
            EventType.PAGE_VIEW: 0.35,
            EventType.PRODUCT_VIEW: 0.3,
            EventType.ADD_TO_CART: 0.18,
            EventType.SEARCH: 0.08,
            EventType.PURCHASE: 0.07,
            EventType.USER_LOGIN: 0.02
        },
        fraud_probability=0.003
    )
}


# Sample product catalog
SAMPLE_PRODUCTS = [
    Product(product_id="prod_001", name="Wireless Headphones", category="Electronics", price=99.99, brand="AudioTech"),
    Product(product_id="prod_002", name="Running Shoes", category="Sports", price=129.99, brand="SportyFeet"),
    Product(product_id="prod_003", name="Coffee Maker", category="Home", price=79.99, brand="BrewMaster"),
    Product(product_id="prod_004", name="Smartphone Case", category="Electronics", price=29.99, brand="ProtectAll"),
    Product(product_id="prod_005", name="Yoga Mat", category="Sports", price=39.99, brand="FlexZone"),
    Product(product_id="prod_006", name="LED Desk Lamp", category="Home", price=49.99, brand="BrightLight"),
    Product(product_id="prod_007", name="Bluetooth Speaker", category="Electronics", price=89.99, brand="SoundWave"),
    Product(product_id="prod_008", name="Kitchen Knife Set", category="Home", price=159.99, brand="SharpEdge"),
    Product(product_id="prod_009", name="Fitness Tracker", category="Sports", price=199.99, brand="HealthSync"),
    Product(product_id="prod_010", name="Laptop Stand", category="Electronics", price=59.99, brand="ErgoPro"),
]

# Sample search queries
SAMPLE_SEARCH_QUERIES = [
    "wireless headphones", "running shoes", "coffee maker", "phone case",
    "yoga equipment", "desk lamp", "bluetooth speaker", "kitchen knife",
    "fitness tracker", "laptop accessories", "home decor", "electronics",
    "sports gear", "kitchen appliances", "office supplies"
]

# Sample locations for geographic distribution
SAMPLE_LOCATIONS = [
    "US-CA", "US-NY", "US-TX", "US-FL", "US-WA", "US-IL",
    "CA-ON", "CA-BC", "UK-LN", "DE-BE", "FR-PA", "AU-NSW", "JP-TK"
]

# Sample user agents
SAMPLE_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X)",
    "Mozilla/5.0 (Android 11; Mobile; rv:68.0)"
]