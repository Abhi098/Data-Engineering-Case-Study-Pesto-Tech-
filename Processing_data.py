from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import avro.schema
import avro.io
import io
import win32api
import re

# Kafka configuration
bootstrap_servers = 'localhost:9092'
ad_impressions_topic = 'ad_impressions'
clicks_conversions_topic = 'clicks_conversions'
avro_data_topic = 'avro_data_topic'
timestamp_regex = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
website_regex = r'https?://(?:www\.)?\w+\.\w+(?:/\S*)?'
# Elasticsearch configuration
es = Elasticsearch("http://localhost:9200")

# Avro schema
schema = avro.schema.Parse(open("bid_request_schema.avsc", "rb").read())

# Create Kafka consumer
consumer = KafkaConsumer(ad_impressions_topic, clicks_conversions_topic, avro_data_topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id='ad_processing_group',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None)

# Function to validate ad impressions
def validate_ad_impression(ad_impression):
    # Example validation: check if ad impression data is not empty
    if not ad_impression:
        win32api.MessageBox("No ad impression data present")
        return False
    if not bool(re.search(timestamp_regex,ad_impression['timestamp'])):
        win32api.MessageBox("Date not correct")
        return False
    
    if not bool(re.search(website_regex, ad_impression['website'])):
        win32api.MessageBox("Website Data not correct")
        return False
    
    return True

# Function to validate clicks/conversions
def validate_click_conversion(click_conversion):
    # Example validation: check if click/conversion data is not empty
    if not click_conversion:
        win32api.MessageBox("No click conversion data present")
        return False
    
    if not bool(re.search(timestamp_regex,click_conversion['timestamp'])):
        win32api.MessageBox("Date not correct")
        return False
    
    if click_conversion['conversion'] in ['Yes','No']:
        win32api.MessageBox("Conversion has garbage values")
        return False
    
    return True

# Function to validate Avro data
def validate_avro_data(avro_data):
    # Example validation: check if Avro data is not empty
    if not avro_data:
        win32api.MessageBox("No avro data present")
        return False
    return True

# Function to filter ad impressions
def filter_ad_impression(ad_impression):
    # Example filtering: filter out invalid ad impressions
    if ad_impression['user_id'] < 0:
        return False
    return True

# Function to filter clicks/conversions
def filter_click_conversion(click_conversion):
    # Example filtering: filter out invalid clicks/conversions
    if click_conversion['user_id'] < 0:
        return False
    return True

# Function to deduplicate data using Elasticsearch
def deduplicate_data(ad_impression, click_conversion):
    # Check if record already exists in Elasticsearch
    if ad_impression:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"user_id": ad_impression['user_id']}},  # Match user ID
                        {"match": {"timestamp": ad_impression['timestamp']}},
                        {"match": {"Website": ad_impression['Website']}},# Match timestamp
                        # Add more conditions to match other fields if necessary
                    ]
                }
            }
        }
        search_result = es.search(index='ad_impressions_index', body=query)
        if search_result['hits']['total']['value'] > 0:
            # If record exists, do not write duplicate
            return None
        else:
            # If record doesn't exist, write to Elasticsearch
            return ad_impression
    else:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"user_id": click_conversion['user_id']}},  # Match user ID
                        {"match": {"Clicks": click_conversion['clicks']}},
                        {"match": {"Campaign": click_conversion['campaign']}},
                        {"match": {"timestamp": click_conversion['timestamp']}},  # Match timestamp
                        {"match": {"Conversion": click_conversion['conversrion']}},
                        # Add more conditions to match other fields if necessary
                    ]
                }
            }
        }
        search_result = es.search(index='clicks_conversions_index', body=query)
        if search_result['hits']['total']['value'] > 0:
            # If record exists, do not write duplicate
            return None
        else:
            # If record doesn't exist, write to Elasticsearch
            return click_conversion
        

# Function to process Avro data
# Function to process Avro data
def process_avro_data(avro_data):
    # Deserialize Avro data
    bytes_reader = io.BytesIO(avro_data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    avro_record = reader.read(decoder)

    # Transform Avro record to JSON format
    json_data = {
        "user_id": avro_record.get("user_id"),
        "ad_creative_id": avro_record.get("ad_creative_id"),
        "timestamp": avro_record.get("timestamp"),
        "bid_amount": avro_record.get("bid_amount")
        # Add more fields as needed
    }
    return json_data


# Function to write data to Elasticsearch
def write_to_elasticsearch(data, index_name):
    # Your code to write data to Elasticsearch here
    es.index(index=index_name, doc_type='_doc', body=data)
    print("Data written to Elasticsearch:", data)

# Main loop to consume and process data
for message in consumer:
    if message.topic == ad_impressions_topic:
        ad_impression = message.value
        # Validate and filter ad impression
        if validate_ad_impression(ad_impression) and filter_ad_impression(ad_impression):
            # Deduplicate ad impression data
            deduplicated_data = deduplicate_data(ad_impression, None)
            if deduplicated_data:
                # Write deduplicated ad impression to Elasticsearch
                write_to_elasticsearch(deduplicated_data, 'ad_impressions_index')
    elif message.topic == clicks_conversions_topic:
        click_conversion = message.value
        # Validate and filter click/conversion
        if validate_click_conversion(click_conversion) and filter_click_conversion(click_conversion):
            # Deduplicate click/conversion data
            deduplicated_data = deduplicate_data(None, click_conversion)
            if deduplicated_data:
                # Write deduplicated click/conversion to Elasticsearch
                write_to_elasticsearch(deduplicated_data, 'clicks_conversions_index')
    elif message.topic == avro_data_topic:
        avro_data = message.value
        # Validate Avro data
        if validate_avro_data(avro_data):
            # Process Avro data
            processed_avro_data = process_avro_data(avro_data)
            # Write processed Avro data to Elasticsearch
            write_to_elasticsearch(processed_avro_data, 'avro_data_index')
