from decimal import Decimal
import os
import time
import logging
from datetime import datetime, timedelta
import psycopg2
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TaxiDataGenerator:
    def __init__(self, db_url):
        self.db_url = db_url
        self.conn = None
        self.cursor = None
        
        self.nyc_bounds = {
            'min_lat': 40.4774,
            'max_lat': 40.9176,
            'min_lon': -74.2591,
            'max_lon': -73.7004
        }
        
        self.popular_locations = [
            # Manhattan
            {'location_id': 230, 'lat': 40.7589, 'lon': -73.9851, 'name': 'Times Square'},
            {'location_id': 186, 'lat': 40.7505, 'lon': -73.9934, 'name': 'Penn Station'},
            {'location_id': 164, 'lat': 40.7527, 'lon': -73.9772, 'name': 'Empire State Building'},
            {'location_id': 43, 'lat': 40.7614, 'lon': -73.9776, 'name': 'Central Park'},
            {'location_id': 87, 'lat': 40.7282, 'lon': -74.0776, 'name': 'Financial District'},
            
            # Brooklyn
            {'location_id': 33, 'lat': 40.6892, 'lon': -73.9442, 'name': 'Brooklyn Heights'},
            {'location_id': 181, 'lat': 40.6782, 'lon': -73.9442, 'name': 'Park Slope'},

            # Queens
            {'location_id': 129, 'lat': 40.7282, 'lon': -73.7949, 'name': 'Jackson Heights'},
            {'location_id': 82, 'lat': 40.7505, 'lon': -73.8803, 'name': 'Elmhurst'},
            
            # JFK Airport
            {'location_id': 132, 'lat': 40.6413, 'lon': -73.7781, 'name': 'JFK Airport'},
            
            # LaGuardia Airport
            {'location_id': 138, 'lat': 40.7769, 'lon': -73.8740, 'name': 'LaGuardia Airport'}
        ]
        
        self.connect_to_db()
        
    def connect_to_db(self):
        """Connect to the PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            logger.info("Connected to the database successfully.")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise
        
    def generate_coordinates(self):
        """Generate pickup and dropoff coordinates"""
        if random.random() < 0.7:
            pickup_location, dropoff_location = random.sample(self.popular_locations, 2)
            
            pu_location_id = pickup_location['location_id']
            do_location_id = dropoff_location['location_id']
            pickup_lat = pickup_location['lat'] 
            pickup_lon = pickup_location['lon']
            dropoff_lat = dropoff_location['lat']
            dropoff_lon = dropoff_location['lon']
        else:
            pu_location_id = random.randint(1, 263)
            do_location_id = random.randint(1, 263)
            pickup_lat = random.uniform(self.nyc_bounds['min_lat'], self.nyc_bounds['max_lat'])
            pickup_lon = random.uniform(self.nyc_bounds['min_lon'], self.nyc_bounds['max_lon'])
            dropoff_lat = random.uniform(self.nyc_bounds['min_lat'], self.nyc_bounds['max_lat'])
            dropoff_lon = random.uniform(self.nyc_bounds['min_lon'], self.nyc_bounds['max_lon'])
            
        return pu_location_id, do_location_id, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate the distance in miles between two points"""
        from math import radians, sin, cos, sqrt, atan2

        R = 3958.8  
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return distance
    
    def generate_taxi_data(self):
        """Genearate a single taxi trip data point"""
        pu_location_id, do_location_id, pickup_lat, pickup_lon, dropoff_lat, dropoff_lon = self.generate_coordinates()
        trip_distance = self.calculate_distance(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
        trip_distance = max(0.1, min(trip_distance, 50.0))
        
        now = datetime.now()
        pickup_datetime = now - timedelta(minutes=random.randint(0, 60), seconds=random.randint(0, 59))
        dropoff_datetime = pickup_datetime + timedelta(minutes=trip_distance * random.uniform(2, 5))
        
        vendor_id = random.choice([1, 2, 6, 7])
        passenger_count = random.randint(1, 4)
        payment_type = random.choices([1, 2, 3, 4], weights=[0.4, 0.4, 0.1, 0.1])[0]
        
        base_fare = 2.50
        per_mile_rate = 2.50
        per_minute_rate = 0.50
        
        distance_fare = trip_distance * per_mile_rate
        time_fare = (dropoff_datetime - pickup_datetime).total_seconds() / 60 * per_minute_rate
        fare_amount = round(base_fare + distance_fare + time_fare, 2)
        
        extra = random.choice([0, 0.50, 1.00]) if random.random() < 0.3 else 0
        mta_tax = 0.50 if fare_amount > 0 else 0
        tip_amount = round(fare_amount * random.uniform(0.15, 0.25), 2) if payment_type == 1 else 0
        tolls_amount = random.choice([0, 5.76, 6.50, 9.75]) if random.random() < 0.1 else 0
        
        total_amount = fare_amount + extra + mta_tax + tip_amount + tolls_amount
        
        return {
            'vendor_id': vendor_id,
            'pickup_datetime': pickup_datetime,
            'dropoff_datetime': dropoff_datetime,
            'passenger_count': passenger_count,
            'trip_distance': Decimal(str(round(trip_distance, 2))),
            'pu_location_id': pu_location_id,
            'do_location_id': do_location_id,
            'payment_type': payment_type,
            'fare_amount': Decimal(str(fare_amount)),
            'extra': Decimal(str(extra)),
            'mta_tax': Decimal(str(mta_tax)),
            'tip_amount': Decimal(str(tip_amount)),
            'tolls_amount': Decimal(str(tolls_amount)),
            'total_amount': Decimal(str(round(total_amount, 2))),
        }
        
    def insert_single_record(self):
        """Insert a single taxi trip record into the database"""
        try:
            record = self.generate_taxi_data()
            
            query = """
            INSERT INTO taxi.trips (
                vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
                trip_distance, pu_location_id, do_location_id, payment_type,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
            ) VALUES (
                %(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s, %(passenger_count)s,
                %(trip_distance)s, %(pu_location_id)s, %(do_location_id)s, %(payment_type)s,
                %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s, %(total_amount)s
            )"""
            
            self.cursor.execute(query, record)
            self.conn.commit()
            
            trip_id = self.cursor.fetchone()[0]
            logger.info(f"Inserted record with trip_id: {trip_id}")
            
            return trip_id
        except Exception as e:
            logger.error(f"Error inserting record into database: {e}")
            self.conn.rollback()
            raise
        
    def insert_batch_records(self, batch_size=100):
        """Insert a batch of taxi trip records into the database"""
        try:
            records = [self.generate_taxi_data() for _ in range(batch_size)]
        
            query = """
            INSERT INTO taxi.trips (
                vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
                trip_distance, pu_location_id, do_location_id, payment_type,
                fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount
            ) VALUES (
                %(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s, %(passenger_count)s,
                %(trip_distance)s, %(pu_location_id)s, %(do_location_id)s, %(payment_type)s,
                %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s, %(total_amount)s
            )"""
            
            self.cursor.executemany(query, records)
            self.conn.commit()
            
            logger.info(f"Inserted batch of {batch_size} records successfully.")
            return True
        except Exception as e:
            logger.error(f"Error inserting batch records into database: {e}")
            self.conn.rollback()
            raise
            
    def updates(self, num_updates=10):
        """Perform updates on existing records"""
        try:
            self.cursor.execute("""
                SELECT id FROM taxi.trips
                ORDER BY created_at DESC
                LIMIT %s
            """, (num_updates,))
            
            trip_ids = [row[0] for row in self.cursor.fetchall()]
            
            if not trip_ids:
                logger.warning("No records found to update")
                return            
            
            for trip_id in trip_ids:
                new_tip_amount = round(random.uniform(0, 10), 2)
                
                self.cursor.execute("""
                    UPDATE taxi.trips
                    SET
                        tip_amount = %s,
                        total_amount = total_amount + %s - tip_amount
                    WHERE id = %s
                """, (Decimal(str(new_tip_amount)), Decimal(str(new_tip_amount)), trip_id))
                
            self.conn.commit()
            logger.info(f"Updated {num_updates} records successfully.")
        except Exception as e:
            logger.error(f"Error updating records: {e}")
            self.conn.rollback()
            raise
        
    def close_connection(self):
        """Close the database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
            
            
def main():
    """Main function to run the data generator"""
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/taxi_db")
    interval_seconds = os.getenv("INTERVAL_SECONDS", 30)
    
    logger.info(f"Starting data generator with interval {interval_seconds} seconds")
    data_generator = TaxiDataGenerator(database_url)
    
    try:
        iteration = 0 
        
        while True:
            iteration += 1
            logger.info(f"Starting iteration {iteration}")
            
            data_generator.insert_batch_records()
            
            if iteration % 5 == 0:
                data_generator.updates()
                
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.error(f"Error during data generation: {e}")
    finally:
        data_generator.close_connection()
        
if __name__ == "__main__":
    main()
        