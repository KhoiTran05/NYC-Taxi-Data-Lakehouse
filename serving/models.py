from pydantic import BaseModel, Field
from typing import Optional, Any, List, Dict
from datetime import datetime, UTC

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    
class DashboardStats(BaseModel):
    total_trips_today: int
    total_revenue_today: float
    avg_fare_today: float
    active_zones: int
    peak_hour: str
    top_zones: List[Dict[str, Any]]
    peak_trips: int
    avg_trip_duration_minutes: float
    avg_trip_distance: float
    
    