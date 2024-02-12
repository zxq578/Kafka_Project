from confluent_kafka import Consumer, KafkaError
import psycopg2
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'foo2',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest'
}

# Database connection configuration for employee_B
db_params = {
    'dbname': 'postgres',
    'user': 'postgres2',
    'password': 'postgres2',
    'host': 'localhost',
    'port': '5433'
}

# Initialize Kafka Consumer
consumer = Consumer(**conf)
consumer.subscribe(['Employee_Changes'])

# Connect to the employee_B database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

def process_message(msg):
    data = json.loads(msg.value().decode('utf-8'))
    action = data.get('action')

    if action == 'insert':
        cursor.execute("INSERT INTO employees (emp_id, first_name, last_name, dob, city) VALUES (%s, %s, %s, %s, %s)",
                       (data['emp_id'], data['first_name'], data['last_name'], data['dob'], data['city']))
    elif action == 'update':
        cursor.execute("UPDATE employees SET first_name=%s, last_name=%s, dob=%s, city=%s WHERE emp_id=%s",
                       (data['first_name'], data['last_name'], data['dob'], data['city'], data['emp_id']))
    elif action == 'delete':
        cursor.execute("DELETE FROM employees WHERE emp_id=%s", (data['emp_id'],))

    conn.commit()

def consume_loop(consumer):
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            process_message(msg)
    finally:
        consumer.close()
        cursor.close()
        conn.close()

consume_loop(consumer)
