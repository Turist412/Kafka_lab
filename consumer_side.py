from websockets.sync.client import connect
import json

from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer("bitcoin", auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    ans = []
    for msg in consumer:
        record = json.loads(msg.value)
        price = float(record["data"]['price'])
        ans.append(price)

    if consumer is not None:
        consumer.close()
    print(sorted(ans, reverse=True)[:10])
