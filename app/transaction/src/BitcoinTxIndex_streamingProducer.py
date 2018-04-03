from kafka import KafkaProducer
import json
from websocket import create_connection

WEBSOCKET_URL = "ws://ws.blockchain.info/inv"
WEBSOCKET_REQUEST = json.dumps({"op": "unconfirmed_sub"})

def produce_Tx_Index(topic):
    """  Get the current information of transaction by creating a connection to "ws://ws.blockchain.info/inv" and seed it to Kafka.

    Arguments:
        topic {string} -- Name of topic to Produce in Kafka

    Returns:
        Void -- Server
    """
    producer = KafkaProducer(acks=1,value_serializer=lambda m: json.dumps(m).encode('ascii'))
    ws = create_connection(WEBSOCKET_URL)

    while True:
        ws.send(WEBSOCKET_REQUEST)
        tx=ws.recv()
        producer.send(topic,tx)
        print("Send ...")

if __name__ == "__main__":
    produce_Tx_Index("transaction_str")
