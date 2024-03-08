# Data-Engineering-Case-Study-Pesto-Tech

AdvertiseX Ad Case Study 

## Assumptions:

### Ads Data:
1. Column Names: Creative Id, User Id, TImestamp, Website
### Clicks and Conversion Data:
2. Column Names: Timestamp, User Id, Campaign, Conversion
### Avro Data:
3. Column Names: User Info, Auction, Targeting
For data ingestion I have assumed that ads data is available to the code via API and for clicks conversion the code is reading dataset from csv.

## Data Ingestion:
### Purpose and Objective:
- The code is designed to demonstrate real-time data streaming using a Kafka Producer.
- It fetches data from an API endpoint, simulates ad impressions, clicks, conversions, and Avro data, and publishes them to Kafka topics.
### Dependencies:
- The script imports required libraries including requests, kafka, json, time, avro.schema, and avro.io.
requests is used for fetching data from the API endpoint.
- kafka is used for interacting with Apache Kafka.
- avro.schema and avro.io are used for Avro serialization.


### Kafka Configuration:
- Kafka broker address (bootstrap_servers) is configured to localhost:9092.
- The Kafka Producer is initialized using KafkaProducer.
### Data Serialization:
- The fetched data from the API and simulated data are serialized to JSON format using json.dumps() for publishing to Kafka topics.
### Avro Serialization:
- Avro schema is defined and parsed using avro.schema.Parse.
- An Avro DatumWriter is created for serializing data according to the schema.
- Data is serialized into Avro binary format using Avro's BinaryEncoder.
### API Data Retrieval:
- The fetch_data_from_api() function is defined to fetch data from the API endpoint.
- It makes a GET request to the API endpoint and returns the JSON response.
- If the response status code is not 200, it prints an error message.
### Data Publication:
- Fetched data from the API is published to the Kafka topic 'ad_impressions'.
- Simulated click/conversion events are published to the Kafka topic 'clicks_conversions'.
- Simulated Avro data is published to the Kafka topic 'avro_data_topic'.
### Simulated Real-time Data Generation:
- A while loop is used for continuous data generation and publishing.
- Simulated data for clicks, conversions, and Avro records are generated within the loop.
### Error Handling and Retry Mechanism:
- If data fetching from the API fails, it waits for 5 seconds before retrying.
- Error messages are printed when data fetching fails.
### Real-time Behavior Simulation:
- time.sleep(1) is used to simulate real-time behavior, generating data every second.
### Producer Cleanup:
- Although not explicitly stated, the script includes cleanup code for flushing and closing the Kafka Producer. However, this code is unreachable due to the infinite loop.
### Logging and Monitoring:
- The script includes print statements for logging and monitoring the data publishing process.


## Data Processing

### Purpose and Objective:
- The code aims to demonstrate the processing and deduplication of real-time data streams using a Kafka Consumer and Elasticsearch.
- It consumes data from Kafka topics related to ad impressions, clicks/conversions, and Avro data, performs validation, filtering, and deduplication, and writes the processed data to Elasticsearch for storage and indexing.
### Dependencies:
- The script imports necessary libraries including KafkaConsumer and json for Kafka interaction, Elasticsearch for data storage, avro.schema and avro.io for Avro serialization, and win32api for displaying messages in Windows.
### Kafka Configuration:
- Kafka broker address (bootstrap_servers) is configured to localhost:9092.
- Kafka Consumer is initialized to consume data from specified topics.
### Elasticsearch Configuration:
- Elasticsearch instance is configured with the address http://localhost:9200.
### Data Validation:
- Functions validate_ad_impression(), validate_click_conversion(), and validate_avro_data() are defined to validate the structure and content of ad impressions, click/conversion events, and Avro data respectively.
- Regular expressions are used to validate timestamps and website URLs.
### Data Filtering:
- Functions filter_ad_impression() and filter_click_conversion() are defined to filter out invalid ad impressions and click/conversion events based on certain criteria such as negative user IDs.
### Deduplication:
- Function deduplicate_data() is defined to check for duplicate records in Elasticsearch based on specified conditions.
- It queries Elasticsearch to check if a record with the same user ID and timestamp already exists.
- If a duplicate record is found, it discards the data; otherwise, it writes the data to Elasticsearch.
### Avro Data Processing:
- Function process_avro_data() deserializes Avro binary data, transforms it into JSON format, and extracts relevant fields such as user ID, ad creative ID, timestamp, and bid amount.
### Data Writing to Elasticsearch:
- Function write_to_elasticsearch() is defined to write processed data to Elasticsearch indices.
- It indexes the data with appropriate index names such as 'ad_impressions_index', 'clicks_conversions_index', and 'avro_data_index'.
### Main Loop for Consuming Data:
- The script iterates through messages received from Kafka topics.
- It distinguishes messages based on the topic and processes them accordingly.
### Error Handling and Messaging:
- Error handling and messaging using win32api.MessageBox() are implemented to notify users about validation failures or other issues.
### Logging and Monitoring:
- The script includes print statements for logging the process of writing data to Elasticsearch.

## Data Analysis

### Purpose and Objective:
- The code aims to demonstrate the analysis of stored data in Elasticsearch, focusing on calculating the correlation between clicks and conversions and extracting statistical metrics.
### Dependencies:
- The script imports the Elasticsearch library to interact with Elasticsearch.
### Elasticsearch Configuration:
- Elasticsearch instance is configured with the address http://localhost:9200.
### Data Analysis Function:
- The function perform_analysis() is defined to analyze stored data in Elasticsearch.
- It calculates the correlation between clicks and conversions and extracts statistical metrics such as mean, median, and standard deviation for both clicks and conversions.
### Aggregation Query:
- An aggregation query is constructed to perform calculations on the stored data.
- Aggregations are used to calculate the sum of clicks and conversions for each user and compute statistical metrics.
### Performing Analysis:
- The aggregation query is executed using es.search() method with specified index and aggregation body.
- Aggregated data including clicks sum, conversions sum, clicks statistics, and conversions statistics are extracted from the search result.
### Correlation Calculation:
- The correlation between clicks and conversions is calculated using the formula: correlation = clicks_sum / conversions_sum.
- If conversions sum is not zero, correlation is calculated; otherwise, it is set to None.
### Extracting Statistical Metrics:
- Statistical metrics such as mean, median, and standard deviation are extracted from the clicks and conversions statistics.
### Printing Analysis Results:
- Analysis results including correlation, clicks mean, clicks median, clicks standard deviation, conversions mean, conversions median, and conversions standard deviation are printed.
### Main Execution:
- The perform_analysis() function is called to initiate the analysis process.
### Error Handling:
- The code does not include explicit error handling. However, Elasticsearch might raise exceptions if there are connection issues or if the specified index does not exist.
### Logging and Monitoring:
- The script includes print statements to log and display the analysis results.


## Created by : Abhishek Pillai
