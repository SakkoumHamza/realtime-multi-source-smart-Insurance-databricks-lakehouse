from confluent_kafka import Producer
import pandas as pd
import json
import time
import glob
import os

def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce(topic, config):
    # create a new producer instance
    producer = Producer(config)
    
    # Path to telematics data folder
    telematics_path = "../../data/telematics/*.parquet"
    
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    # Get all parquet files
    parquet_files = glob.glob(telematics_path)
    
    if not parquet_files:
        print("No parquet files found in telematics folder!")
        return
        
    print(f"Found {len(parquet_files)} parquet files")
    
    # Read and stream data from each parquet file
    for file_path in parquet_files:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        try:
            # Read parquet file
            df = pd.read_parquet(file_path)
            
            # Stream each row as a message
            for index, row in df.iterrows():
                # Handle timestamp conversion
                event_ts = row.get("event_timestamp")
                if pd.notna(event_ts):
                    if isinstance(event_ts, str):
                        # Convert datetime string to timestamp
                        event_timestamp = int(pd.to_datetime(event_ts).timestamp())
                    else:
                        event_timestamp = int(event_ts)
                else:
                    event_timestamp = int(time.time())
                
                # Create telematics message matching your DLT schema
                doc = {
                    "chassis_no": str(row.get("chassis_no", f"CHASSIS_{index}")),
                    "latitude": float(row.get("latitude", 0.0)) if pd.notna(row.get("latitude")) else 0.0,
                    "longitude": float(row.get("longitude", 0.0)) if pd.notna(row.get("longitude")) else 0.0,
                    "event_timestamp": event_timestamp,
                    "speed": float(row.get("speed", 0.0)) if pd.notna(row.get("speed")) else 0.0,
                    "ingest_time": int(time.time())
                }
                
                # Use chassis_no as message key
                producer.produce(
                    topic, 
                    key=doc["chassis_no"], 
                    value=json.dumps(doc), 
                    callback=delivery_report
                )
                
                # Small delay to simulate real-time streaming
                time.sleep(0.1)
            
            producer.flush()
            print(f"Produced {len(df)} messages from {os.path.basename(file_path)}")
            
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            
    print("Finished streaming all telematics data")

def main():
    config = read_config()
    topic = "claims"  # Using existing claims topic
    
    produce(topic, config)

if __name__ == "__main__":
    main()