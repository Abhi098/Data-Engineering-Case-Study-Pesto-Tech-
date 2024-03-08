# -*- coding: utf-8 -*-
import requests
from kafka import KafkaProducer
import json
import time
import avro.schema
import avro.io
import io

# Kafka configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


schema = avro.schema.Parse(open("bid_request_schema.avsc", "rb").read())
avro_writer = avro.io.DatumWriter(schema)
avro_data = io.BytesIO()


# Function to fetch data from the API
def fetch_data_from_api():
    # Replace 'api_endpoint' with the actual API endpoint URL
    response = requests.get('api_endpoint')
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from API:", response.status_code)
        return None

# Simulated real-time data generation from API
while True:
    # Fetch data from API
    api_data = fetch_data_from_api()
    if api_data:
        # Send fetched data to Kafka topics
        for data_item in api_data:
            producer.send('ad_impressions', value=data_item)
            print("Published Ad Impression data from API:", data_item)
    else:
        # If failed to fetch data from API, wait for some time before retrying
        time.sleep(5)  # Retry after 5 seconds
    
    # Simulated clicks and conversions data   
    click_conversion = {"user_id": 1001, "ad_creative_id": 1, "event_timestamp": int(time.time()), "conversion_type": "signup"}
    producer.send('clicks_conversions', value=click_conversion)
    print("Published click/conversion event:", click_conversion)

    # Simulated Avro data
    avro_record = {"user_id": 1001, "ad_creative_id": 1, "timestamp": int(time.time()), "bid_amount": 10.5}
    avro_data.seek(0)
    avro_data.truncate(0)
    encoder = avro.io.BinaryEncoder(avro_data)
    avro_writer.write(avro_record, encoder)
    avro_data_payload = avro_data.getvalue()
    producer.send('avro_data_topic', value=avro_data_payload)
    print("Published Avro data:", avro_record)
    
    
    
    
    
    
    # Sleep for some time to simulate real-time behavior
    time.sleep(1)  # Simulate real-time data generation every second

# Flush and close the producer (this will never execute as the loop is infinite)
producer.flush()
producer.close()

