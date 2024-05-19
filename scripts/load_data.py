import os
import psycopg2
from faker import Faker
from glob import glob
import geopandas as gpd

def main():
    # Initialize Faker
    fake = Faker()

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="tfmv",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    batch_size = 1000
    insert_count = 0
    batch = []

    # Traverse each .geojson file in all subdirectories
    for filepath in glob("/Users/thomasmcgeehan/FuzzyMatchFinder/FuzzyMatchFinder/data/us/**/*.geojson", recursive=True):
        print(f"Processing file: {filepath}")  # Debug print

        try:
            # Load the .geojson file using geopandas
            gdf = gpd.read_file(filepath)
            print(gdf.head())  # Debug print to understand the structure

            for _, row in gdf.iterrows():
                # Check if 'properties' exist
                if 'properties' in row:
                    props = row['properties']
                    # Access properties fields
                    hash_value = props.get('hash', '')
                    number = props.get('number', '')
                    street = props.get('street', '')
                    unit = props.get('unit', '')
                    city = props.get('city', '')
                    district = props.get('district', '')
                    region = props.get('region', '')
                    postcode = props.get('postcode', '')
                else:
                    # Directly access fields if not nested under 'properties'
                    hash_value = row.get('hash', '')
                    number = row.get('number', '')
                    street = row.get('street', '')
                    unit = row.get('unit', '')
                    city = row.get('city', '')
                    district = row.get('district', '')
                    region = row.get('region', '')
                    postcode = row.get('postcode', '')

                # Skip records without a valid city
                if not city:
                    continue

                coordinates = row.geometry.centroid.coords[0]  # Use centroid for a single coordinate pair

                # Generate fake names
                first_name = fake.first_name()
                last_name = fake.last_name()

                # Prepare the data for batch insert
                batch.append((
                    hash_value, number, street, unit, city, district, region, postcode, 
                    first_name, last_name, coordinates[1], coordinates[0]
                ))

                # Insert records in batches
                if len(batch) >= batch_size:
                    insert_count += len(batch)
                    cur.executemany("""
                        INSERT INTO addresses (hash, number, street, unit, city, district, region, postcode, first_name, last_name, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    batch = []  # Clear the batch
                    print(f"{insert_count} records inserted.")
        except Exception as e:
            print(f"Error processing file {filepath}: {e}")  # Debug print

    # Insert any remaining records in the batch
    if batch:
        insert_count += len(batch)
        cur.executemany("""
            INSERT INTO addresses (hash, number, street, unit, city, district, region, postcode, first_name, last_name, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch)
        conn.commit()
        print(f"{insert_count} records inserted.")

    # Close the connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
