from config import settings
from database import DatabaseService
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, Query, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, UTC
from models import APIResponse, DashboardStats
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if settings:
    db_service = DatabaseService(settings.database_url)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Lakehouse Analytics API starting up ...")
    yield
    logger.info("Lakehouse Analytics API shutting down ...")
    
app = FastAPI(
    title="Lakehouse Analytics API",
    description="REST API for real-time and historical taxi analytics",
    version="1.0.0",
    docs_url="/doc",
    redoc_url="/redoc",
    lifespan=lifespan
)

api_key_header = APIKeyHeader(name="API-KEY", auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    if settings and api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(UTC)
    }
    
@app.get("/api/v1/dashboard/stats", response_model=DashboardStats)
async def get_dashboard_stats(api_key: str = Depends(get_api_key)):
    """Get statistics for dashboard"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        stats = db_service.get_dashboard_stats()
        return DashboardStats(**stats)
    except Exception as e:
        logger.exception(f"Error getting dashboard statistics")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/v1/trips/recent")
async def get_recent_trips(
    limit: int = Query(100, ge=1, le=1000),
    hours_back: int = Query(24, ge=1, le=168),
    api_key: str = Depends(get_api_key)
):
    """Get recent trips data"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        recent_trips = db_service.get_recent_trips(limit, hours_back)
        results = recent_trips.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(results)} recent trip records",
            data=results
        )
    except Exception as e:
        logger.error(f"Error getting recent trip records: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/zones")
async def get_zone_analytics(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    api_key: str = Depends(get_api_key)
):
    """Get zone metrics for analytics"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        zones_metrics = db_service.get_zone_metrics(start_date, end_date)
        results = zones_metrics.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(results)} zones records",
            data=results
        )
    except Exception as e:
        logger.error(f"Error getting zone metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/v1/analytics/time-series")
async def get_time_series(
    metric: str = Query("trip_count", pattern="^(trip_count|revenue|avg_fare)$"),
    days_back: int = Query(7, ge=1, le=30),
    api_keu : str = Depends(get_api_key)
):
    """Get time series metrics"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        metric_mapping = {
            "trip_count": "total_trips",
            "revenue": "total_revenue",
            "avg_fare": "avg_fare"
        }
        
        if metric not in metric_mapping:
            raise HTTPException(status_code=400, detail="Invalid metric")
        
        time_series_metrics = db_service.get_hourly_trip_metrics(days_back)
        
        series_data = []
        for _, row in time_series_metrics.iterrows():
            series_data.append({
                "timestamp": row["hour_timestamp"].isoformat(),
                "value": float(row[metric_mapping[metric]])
            })
            
        return APIResponse(
            success=True,
            message=f"Time series data for {metric}",
            data={
                "metric": metric,
                "unit": "count" if metric == "trip_count" else "currency",
                "series": series_data
            }
        )
    except Exception as e:
        logger.error(f"Error getting time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/v1/realtime/activity")
async def get_real_time_activity(
    minutes_back: int = Query(60, ge=5, le=120),
    api_key: str = Depends(get_api_key)
):
    """Get realtime activity stats"""
    try:
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service not available")
        
        activity_df = db_service.get_real_time_activity(minutes_back)
        results = activity_df.to_dict('records')
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(results)} realtime activity records",
            data=results
        )
    except Exception as e:
        logger.error(f"Error getting realtime activity records: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
