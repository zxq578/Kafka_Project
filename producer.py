import json
import time
import psycopg2
from kafka import KafkaProducer

# Database configuration
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Kafka Configuration
producer = KafkaProducer(bootstrap_servers='localhost:29092',
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

def fetch_cdc_data(cursor, last_processed_id):
    # Fetch new changes since the last processed ID
    cursor.execute("""
        SELECT emp_id, first_name, last_name, dob, city, action 
        FROM employees_cdc 
        WHERE emp_id > %s
        ORDER BY emp_id ASC;
    """, (last_processed_id,))
    return cursor.fetchall()

def publish_to_kafka(producer, records):
    topic = 'Employee_Changes'
    for record in records:
        message = {
            "emp_id": record[0],
            "first_name": record[1],
            "last_name": record[2],
            "dob": str(record[3]),
            "city": record[4],
            "action": record[5]
        }
        producer.send(topic, value=message)
        print(f"Published record to {topic}: {message}")
    producer.flush()

def main():
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    last_processed_id = 0

    try:
        while True:
            # Fetch and publish new CDC data
            records = fetch_cdc_data(cursor, last_processed_id)
            if records:
                publish_to_kafka(producer, records)
                last_processed_id = records[-1][0]  # Update last processed ID
            time.sleep(10)  # Polling interval
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
