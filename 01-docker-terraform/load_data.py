import os
import gzip
import requests
import psycopg2
from psycopg2 import sql

# Configuration
DATA_DIR = "data"
CSV_URLS = {
    "green_trips": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz",
    "taxi_zones": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
}

# PostgreSQL configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ny_taxi",
    "user": "postgres",
    "password": "postgres"
}

def download_file(url: str, file_path: str):
    """Download a file from a URL"""
    response = requests.get(url, stream=True)
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def decompress_gz(gz_path: str, output_path: str):
    """Unzip .gz files"""
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())

def main():
    # Create directory if it does not exist
    os.makedirs(DATA_DIR, exist_ok=True)

    # Download files
    print("Downloading data...")
    for name, url in CSV_URLS.items():
        file_ext = ".csv.gz" if "green" in name else ".csv"
        file_path = os.path.join(DATA_DIR, f"{name}{file_ext}")
        download_file(url, file_path)
        print(f"Downloaded: {file_path}")

    # Unzip green_trips
    gz_path = os.path.join(DATA_DIR, "green_trips.csv.gz")
    csv_path = os.path.join(DATA_DIR, "green_trips.csv")
    decompress_gz(gz_path, csv_path)
    print(f"Unzipped: {csv_path}")

    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create tables if do not exist
    print("Creating tables...")
    cursor.execute("""
        DROP TABLE IF EXISTS green_trips, taxi_zones;
        
        CREATE TABLE taxi_zones (
            LocationID INT PRIMARY KEY,
            Borough VARCHAR,
            Zone VARCHAR,
            service_zone VARCHAR
        );

        CREATE TABLE green_trips (
            VendorID INT,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag CHAR(1),
            RatecodeID INT,
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance NUMERIC,
            fare_amount NUMERIC,
            extra NUMERIC,
            mta_tax NUMERIC,
            tip_amount NUMERIC,
            tolls_amount NUMERIC,
            ehail_fee NUMERIC,
            improvement_surcharge NUMERIC,
            total_amount NUMERIC,
            payment_type INT,
            trip_type INT,
            congestion_surcharge NUMERIC
        );
    """)
    conn.commit()

    # Load data
    print("Loading taxi_zones...")
    with open(os.path.join(DATA_DIR, "taxi_zones.csv"), 'r') as f:
        cursor.copy_expert(
            sql.SQL("COPY taxi_zones FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"),
            f
        )

    print("Loading green_trips...")
    with open(csv_path, 'r') as f:
        cursor.copy_expert(
            sql.SQL("COPY green_trips FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"),
            f
        )

    conn.commit()
    print("Data loaded successfully!")

    # Cleaning
    cursor.close()
    conn.close()
    os.remove(gz_path)  # Delete compressed file
    
if __name__ == "__main__":
    main()