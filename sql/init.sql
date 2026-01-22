CREATE SCHEMA IF NOT EXISTS taxi;

CREATE TABLE IF NOT EXISTS taxi.trips (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance DECIMAL,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DECIMAL,
    extra DECIMAL,
    mta_tax DECIMAL,
    tip_amount DECIMAL,
    tolls_amount DECIMAL,
    total_amount DECIMAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime ON taxi.trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_datetime ON taxi.trips (dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_pu_location_id ON taxi.trips (pu_location_id);
CREATE INDEX IF NOT EXISTS idx_trips_do_location_id ON taxi.trips (do_location_id);
CREATE INDEX IF NOT EXISTS idx_trips_created_at ON taxi.trips (created_at);

CREATE TABLE IF NOT EXISTS taxi.payment_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    description TEXT
);

INSERT INTO taxi.payment_types (id, name, description) VALUES
(1, 'Credit Card', 'Payment made using a credit card'),
(2, 'Cash', 'Payment made using cash'),
(3, 'No Charge', 'No charge for the trip'),
(4, 'Dispute', 'Payment is under dispute'),
(5, 'Unknown', 'Payment type is unknown'),
(6, 'Voided Trip', 'Trip was voided');

CREATE TABLE IF NOT EXISTS taxi.vendors (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

INSERT INTO taxi.vendors (id, name, description) VALUES
(1, 'Creative Mobile Technologies, LLC', 'A leading provider of mobile technology solutions'),
(2, 'Curb Mobility', 'A global leader in payment and commerce solutions'),
(6, 'Myle Technologies Inc', 'A technology company specializing in transportation solutions'),
(7, 'Helix', 'A technology company focused on innovative transportation services');


CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_trips_updated_at 
BEFORE UPDATE ON taxi.trips 
FOR EACH ROW 
EXECUTE FUNCTION update_updated_at_column();

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'debezium') THEN
        CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE taxi_db TO debezium;
GRANT USAGE ON SCHEMA taxi TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA taxi TO debezium;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA taxi TO debezium;

ALTER TABLE taxi.trips REPLICA IDENTITY FULL;
ALTER TABLE taxi.payment_types REPLICA IDENTITY FULL;
ALTER TABLE taxi.vendors REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_publication FOR TABLE taxi.trips, taxi.payment_types, taxi.vendors;