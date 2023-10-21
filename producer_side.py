from websockets.sync.client import connect
import json
from kafka import KafkaProducer


def send_msg(producer, topic_name, key, value):
    key_bytes = bytes(key, encoding='utf-8')
    value_bytes = bytes(value, encoding='utf-8')
    producer.send(topic_name, key=key_bytes, value=value_bytes)
    producer.flush()


def connect_kafka():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    return producer


server_ip = "wss://ws.bitstamp.net"

channgel_name = "live_orders_btcusd"

subscription_msg = {
    "event": "bts:subscribe",
    "data": {
        "channel": channgel_name
    }
}

unsubscription_msg = {
    "event": "bts:unsubscribe",
    "data": {
        "channel": channgel_name
    }
}

if __name__ == '__main__':
    with connect(server_ip) as websocket:
        websocket.send(json.dumps(subscription_msg))
        message = websocket.recv()
        print(f"Received: {message}")

        print("-" * 20)
        producer = connect_kafka()
        for _ in range(50):
            message = websocket.recv()
            print(f"Received: {message}")
            send_msg(producer, 'bitcoin', 'raw', message)
        print("-" * 20)
        if producer is not None:
            producer.close()

        websocket.send(json.dumps(unsubscription_msg))