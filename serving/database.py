from sqlalchemy import create_engine, text
from typing import Any, Dict
from datetime import datetime
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=300)
        
    def execute_query(self, query: str, params: Dict[str, Any]  = None) -> pd.DataFrame:
        """Execute SQL query and return pandas Dataframe"""
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection, params=params)
                return df
        except Exception:
            logger.exception("Error during executing query")
            raise
        
    def get_recent_trips(self, limit: int = 100, hours_back: int = 24) -> pd.DataFrame:
        """Get recent taxi trips"""
        query = """
            SELECT
                id,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pu_location_id,
                do_location_id,
                fare_amount,
                tip_amount,
                total_amount,
                payment_type
            FROM taxi.trips
            WHERE pickup_datetime >= NOW() - (:hours_interval * INTERVAL '1 hour')
            LIMIT :limit
        """
        
        return self.execute_query(query, {
            "hours_interval": hours_back,
            "limit": limit
        })
        
    def get_zone_metrics(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get zone metrics between date range"""
        query = """
            WITH pickup_aggs AS (
                SELECT
                    pu_location_id AS zone_id,
                    COUNT(*) AS pickup_count,
                    AVG(fare_amount) as avg_fare,
                    SUM(total_amount) as total_revenue,
                    AVG(trip_distance) as avg_distance,
                    COUNT(CASE WHEN EXTRACT(hour FROM pickup_datetime) BETWEEN 17 AND 19 THEN 1 END) as peak_trips
                FROM taxi.trips
                WHERE pickup_datetime BETWEEN :start_date AND :end_date
                GROUP BY pu_location_id
            ),
            dropoff_aggs AS (
                SELECT
                    do_location_id AS zone_id,
                    COUNT(*) AS dropoff_count
                FROM taxi.trips
                WHERE pickup_datetime BETWEEN :start_date AND :end_date
                GROUP BY do_location_id
            )
            SELECT
                pa.zone_id,
                COALESCE(tz.zone_name, 'Unknown') AS zone_name,
                COALESCE(tz.borough, 'Unknown') AS borough,
                pa.pickup_count AS total_pickups,
                COALESCE(da.dropoff_count, 0) AS total_dropoffs,
                pa.avg_fare,
                pa.total_revenue,
                pa.avg_distance,
                pa.peak_trips
            FROM pickup_aggs pa
            LEFT JOIN dropoff_aggs da ON pa.zone_id = da.zone_id
            LEFT JOIN taxi.taxi_zones tz ON pa.zone_id = tz.location_id  
        """
        
        return self.execute_query(query, {
            "start_date": start_date,
            "end_date": end_date
        })
        
    def get_hourly_trip_metrics(self, days_back: int = 7) -> pd.DataFrame:
        """Get hourly trip aggregations"""
        query = """
            SELECT
                DATE_TRUNC('hour', pickup_datetime) AS hour_timestamp,
                COUNT(*) AS total_trips,
                AVG(fare_amount) AS avg_fare,
                SUM(total_amount) AS total_revenue
            FROM taxi.trips
            WHERE pickup_datetime >= NOW() - (:days_interval * INTERVAL '1 day')
            GROUP BY date_trunc('hour', pickup_datetime)
            ORDER BY hour_timestamp
        """
        
        return self.execute_query(query, {
            "days_interval": days_back
        })
        
    def get_real_time_activity(self, minutes_back: int = 60) -> pd.DataFrame:
        """Get real time activity by zone"""
        query = """
            WITH recent_activity AS (
                SELECT 
                    pu_location_id as zone_id,
                    COUNT(*) as pickup_count,
                    SUM(total_amount) as revenue,
                    AVG(EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime))/60) as avg_trip_duration_minutes
                FROM taxi.trips
                WHERE pickup_datetime >= NOW() - (:minutes_interval * INTERVAL '1 minute')
                    AND pu_location_id IS NOT NULL
                    AND dropoff_datetime IS NOT NULL
                    AND fare_amount > 0
                GROUP BY pu_location_id
            )
            SELECT 
                ra.zone_id,
                COALESCE(tz.zone_name, 'Unknown Zone') as zone_name,
                tz.latitude,
                tz.longitude,
                NOW() as activity_timestamp,
                LEAST((ra.pickup_count * 10 + ra.revenue / 10)::numeric, 100)::numeric(5,2) as activity_score,
                ra.pickup_count,
                ROUND(ra.revenue::numeric, 2) as revenue_last_minutes,
                ROUND(ra.avg_trip_duration_minutes::numeric, 1) as avg_trip_duration_minutes
            FROM recent_activity ra
            LEFT JOIN taxi.taxi_zones tz ON ra.zone_id = tz.location_id
            WHERE ra.pickup_count > 0
            ORDER BY activity_score DESC
        """
        
        return self.execute_query(query, {
            "minutes_interval": minutes_back
        })
        
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get key statistics for dashboard"""
        current_stats_query = """
            SELECT
                COUNT(*) AS total_trips,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(AVG(fare_amount), 0) as avg_fare,
                COUNT(DISTINCT pu_location_id) as active_zones,
                AVG(EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime))/60) as avg_trip_duration_minutes,
                COUNT(
                    CASE 
                        WHEN EXTRACT(hour FROM pickup_datetime) BETWEEN 7 AND 9
                        OR EXTRACT(hour FROM pickup_datetime) BETWEEN 17 AND 19
                        THEN 1
                    END
                ) AS peak_trips,
                AVG(trips.trip_distance) AS avg_trip_distance
            FROM taxi.trips
            WHERE DATE(pickup_datetime) = CURRENT_DATE
                AND fare_amount > 0
        """
        
        current_stats = self.execute_query(current_stats_query).iloc[0].to_dict()
        
        peak_query = """
            SELECT
                EXTRACT(hour FROM pickup_datetime) AS hour,
                COUNT(*) AS trip_count
            FROM taxi.trips
            WHERE DATE(pickup_datetime) = CURRENT_DATE
            GROUP BY EXTRACT(hour FROM pickup_datetime)
            ORDER BY trip_count DESC
            LIMIT 1
        """
        
        peak_results = self.execute_query(peak_query)
        peak_hour = f"{peak_results.iloc[0]['hour']}:00" if not peak_results.empty else "N/A"
        
        top_zones_query = """
            SELECT
                COALESCE(tz.zone_name, 'Unknown') AS zone_name,
                COUNT(*) AS trips,
                SUM(t.total_amount) AS revenue
            FROM taxi.trips t
            LEFT JOIN taxi.taxi_zones tz ON t.pu_location_id = tz.location_id
            WHERE DATE(t.pickup_datetime) = CURRENT_DATE
            GROUP BY tz.zone_name
            ORDER BY revenue DESC
            LIMIT 5
        """
        
        top_zones = self.execute_query(top_zones_query).to_dict('records')
        
        return {
            'total_trips_today': int(current_stats['total_trips']),
            'total_revenue_today': float(current_stats['total_revenue']),
            'avg_fare_today': float(current_stats['avg_fare']),
            'active_zones': int(current_stats['active_zones']),
            'peak_hour': peak_hour,
            'top_zones': top_zones,
            'peak_trips': int(current_stats['peak_trips']),
            'avg_trip_duration_minutes': float(current_stats['avg_trip_duration_minutes']),
            'avg_trip_distance': float(current_stats['avg_trip_distance'])
        }
        
        
        
        
        
    
        
        
        
    