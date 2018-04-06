from kafka import KafkaProducer
import json
from websocket import create_connection
import datetime
from BitcoinMinersIndex_historicalProducer import connectionToAPI

WEBSOCKET_URL = "ws://ws.blockchain.info/inv"
WEBSOCKET_REQUEST = json.dumps({"op":"blocks_sub"})
DEFAULT_HOST = "blockchain.info"
URI_TRANSACTIONS = "/fr/rawblock/"

def produce_Block_Index(topic):
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

        getFirstTx = connectionToAPI(DEFAULT_HOST, URI_TRANSACTIONS + str(id_hash))
        # there are transaction that don't have the 'tx' field: so in case of that, I manage this.
        if getFirstTx.has_key('tx'):
            producer.send(topic,getFirstTx['tx'][0])
            print(".",end="",flush=True)
        else:
            print("#",end="",flush=True)
            print(getFirstTx)
if __name__ == "__main__":
    produce_Block_Index("mineur_str")
