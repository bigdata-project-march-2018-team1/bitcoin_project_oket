from kafka import KafkaProducer
import json
from websocket import create_connection
import datetime
from BitcoinMinersIndex_historicalProducer import getFirstTx

WEBSOCKET_URL = "ws://ws.blockchain.info/inv"
WEBSOCKET_REQUEST = json.dumps({"op":"blocks_sub"})

def produce_Tx_Index(topic):
    """  Get the current information of transaction by creating a connection to "ws://ws.blockchain.info/inv" and seed it to Kafka.

    Arguments:
        topic {string} -- Name of topic to Produce in Kafka

    Returns:
        Void -- Server
    """
    producer = KafkaProducer(acks=1,max_request_size=10000000,value_serializer=lambda m: json.dumps(m).encode('ascii'))
    ws = create_connection(WEBSOCKET_URL)

    while True:
        ws.send(WEBSOCKET_REQUEST)
        block=ws.recv()

        id_hash=json.loads(block)['x']['hash']
        
        first_tx=getFirstTx(id_hash)

        producer.send(topic,first_tx)

if __name__ == "__main__":
    produce_Tx_Index("mineur_str")
