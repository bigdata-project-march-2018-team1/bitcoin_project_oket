from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import datetime

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers

from elastic_storage import http_auth

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
BITCOIN_TO_SATOSHI = 100000000

def add_real_time_tx(name,realTimeData):
    """
    Add the data contains in 'realTimeData' to ElasticSearchBase, represented by 'es', of table indice 'index'.

    Arguments:
        realTimeData { list[directories] } -- list of dictionnary
        index {string} -- name of indice of ElasticDataBase
        es {ElasticSearch} -- Elasticsearch object

        Returns:
        void -- Put data to ElasticDataBase
    """
    actions = [
        {
            "_index": name,
            "_type": "doc",
            "_id": int(data['tx_index']),
            "_source":{
                "type": "real-time",
                "value": float(data['value'])/BITCOIN_TO_SATOSHI,
                "time": {
                    'path': data['time'],
                    'format': DATE_FORMAT
                    }
            }
        }
        for data in realTimeData
    ]
    helpers.bulk(connections.get_connection(), actions)

def timestampsToString(timestamps):
    return str(datetime.datetime.fromtimestamp(int(timestamps)).strftime(DATE_FORMAT))

def filtre_tx(dico):
    """ Filter just after the input of the streaming to get the time, the value and the tx_index of each transaction.

    Arguments:
        dico {dictionnary} -- contains many informations what need to be filtered

    Returns:
        dictionnary -- filtered dictionnary
    """
    #js=json.loads(dico[1])['x']
    result = {
        'time': timestampsToString(dico['time']),
        'tx_index':dico['tx_index'],
        'value': sum( (dico['inputs'][i]['prev_out']['value'] for i in list(range(len(dico['inputs'])))) )
        }

    return result

def consume_Tx_Index(topic,index,master="local[2]", appName="CurrentTransaction",elasticsearch_host="localhost" ,elasticsearch_port=9200, kafkaConsumer_host="localhost",kafkaConsumer_port=2181):
    """ Get the current information of transaction by a KafkaProducer and SparkStreaming/KafkaUtils, theses informations are send to an ElasticSearchBase.

    Arguments:
        topic {string} -- Name of topic to Consume in Kafka
        index {string} -- Name of index of ElasticSearchDataBase
        master {string} -- Set master URL to connect to
        appName {string} -- Set application name
        elasticsearch_host {string} -- ElasticSearch host to connect for
        elasticsearch_port {int} -- ElasticSearch port to connect for
        kafkaConsumer_host {string} -- kafkaStream consumer to connect for getting streamingPrice
        kafkaConsumer_port {int} -- kafkaStream port to connect for

    Returns:
        void -- Send to ElasticSearchDataBase data.
    """
    sc = SparkContext(master, appName)
    strc = StreamingContext(sc, 5)

    kafkaStream = KafkaUtils.createStream(strc, kafkaConsumer_host+':'+str(kafkaConsumer_port), 'SparkStreaming', {topic: 1},valueDecoder=lambda m:json.loads(m.decode('ascii')),kafkaParams = {"metadata.broker.list": 'localhost:9092'})\
                            .map(lambda dico:filtre_tx(json.loads(dico[1])['x']))

    kafkaStream.foreachRDD(lambda rdd: add_real_time_tx(index,rdd.collect()))

    strc.start()
    strc.awaitTermination()

if __name__ == "__main__":
    connections.create_connection(hosts='localhost')#, http_auth=http_auth('elastic'))
    consume_Tx_Index('transaction_str','bitcoin_tx')
