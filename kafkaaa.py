from confluent_kafka import Producer
import requests
import json

# Kafka broker configuration
conf = {
    'bootstrap.servers': 'sandbox-hdp.hortonworks.com:6667',  # Replace with your Kafka broker(s)
}

# Kafka topic
topic = 'testallo'  # Replace with the name of your Kafka topic

# Create Kafka producer
producer = Producer(conf)

# Function to fetch data from the API
def fetch_data_from_api():
    # Example API endpoint
    api_url = 'http://api.open-notify.org/iss-now.json'
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Main function to produce data to Kafka topic
def produce_data():
    while True:
        data = fetch_data_from_api()
        if data:
            # Produce data to Kafka topic
            producer.produce(topic,json.dumps(data).encode('utf-8'))
            producer.flush()
            print("Data published to Kafka topic:", data)
        else:
            print("Failed to fetch data from the API")

# Run the producer
if __name__ == '__main__':
    produce_data()

