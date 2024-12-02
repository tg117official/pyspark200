from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='18.188.225.10:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/send', methods=['POST'])
def send_to_kafka():
    data = request.json
    producer.send('test-topic', data)
    producer.flush()
    return jsonify({"status": "success", "data": data}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

# curl -X POST http://localhost:5000/send -H "Content-Type: application/json" -d '{"key":"My_key", "value":"1,ABC,45"}'