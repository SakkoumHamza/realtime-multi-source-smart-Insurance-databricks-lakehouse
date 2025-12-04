import os
import pyarrow.parquet as pq
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
import configparser


# Define the directory containing your Parquet files
parquet_directory = "data/telematics"  # e.g., './data/'


# Read connection string from configuration file
def read_connection_string():
    """Read connection string from conf.conf file"""
    config = configparser.ConfigParser()
    config.read('conf.conf')
    
    # If the config file has sections, use DEFAULT section
    # Otherwise, read the raw connection_string value
    try:
        return config['DEFAULT']['connection_string']
    except KeyError as exc:
        # Handle case where there are no sections in the config file
        with open('conf.conf', 'r', encoding='utf-8') as f:
            content = f.read().strip()
            # Extract connection string value after the = sign
            if 'connection_string' in content:
                return content.split('=', 1)[1].strip().strip('"')
        raise ValueError("Could not find connection_string in conf.conf") from exc
    
# Event Hub configuration
connection_str = read_connection_string()
event_hub_name = "claims"

# Create an Event Hub client
producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=event_hub_name)

# Function to send DataFrame to Event Hub
def send_to_event_hub(df):
    with producer:
        for index, row in df.iterrows():
            message = row.to_json()  # You can customize the message format here
            event_data = EventData(message)
            producer.send_batch([event_data])  # Send the messages in a batch
        print("Data sent to Event Hub.")

# Loop through all Parquet files in the directory
for filename in os.listdir(parquet_directory):
    if filename.endswith(".parquet"):
        file_path = os.path.join(parquet_directory, filename)
        print(f"Reading {file_path}...")

        # Read the Parquet file into a Pandas DataFrame
        table = pq.read_table(file_path)
        df = table.to_pandas()

        # Send the data to Event Hub
        send_to_event_hub(df)

print("All files processed and sent to Event Hub.")

