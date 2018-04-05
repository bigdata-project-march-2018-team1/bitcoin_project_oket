from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import datetime

from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch, helpers
from BitcoinMinersIndex_historicalConsumer import filter_tx
from BitcoinMinersIndex_historicalProducer import getFirstTx
from elastic_storage import http_auth

import logging

TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
BITCOIN_TO_SATOSHI = 100000000

#la
import time

#fin
def add_real_time_tx(name,realTimeData):
    """
    Add the data contains in 'realTimeData' to ElasticSearchBase of table indice 'index'.

    Arguments:
        realTimeData { list[directories] } -- list of dictionnary
        index {string} -- name of indice of ElasticDataBase

        Returns:
        void -- Put data to ElasticDataBase
    """

    millis = int(round(time.time() * 1000))
    actions = [
        {
            "_index": name,
            "_type": "doc",
            "_id": int(data['tx_index']),
            "_source":{
                "mineur_id": int(data['mineur_id']),
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
    print("time elas est:")
    print(int(round(time.time() * 1000))-millis)

def timestampsToString(timestamps):
    return str(datetime.datetime.fromtimestamp(int(timestamps)).strftime(DATE_FORMAT))


def filtre_miner(block_id):
    """ Filter just after the input of the streaming to get the time, the value and the tx_index of each transaction.

    Arguments:
        dico {dictionnary} -- contains many informations what need to be filtered

    Returns:
        dictionnary -- filtered dictionnary
    """
    first_tx=getFirstTx(block_id)
    print("test critic")
    print(type(first_tx))
    print(first_tx)
    return filter_tx(first_tx)

def convert(m):
    millis=int(round(time.time() * 1000))
    mbis=json.loads(m.decode('ascii'))
    print("time convert est:")
    print(int(round(time.time() * 1000))-millis)
    return mbis

def send(rdd, config):
    """ Send to elastic

    Arguments:
        rdd {RDD} -- Data to send to elastic

    Keyword Arguments:
        config {dict} -- Configuration
    """

    data_tx = rdd.collect()
    if data_tx:
        connections.create_connection(
            hosts=config['elasticsearch']['hosts'], http_auth=http_auth('elastic'))
        add_streaming_miners(data_tx)
        logging.info("Data sent to Elastic")
def add_streaming_miners(dataset):
    ''' Get data from the API between two dates '''
    ''' Call to bulk api to store the data '''
    actions = [
        {
            "_index": "bitcoin_miners",
            "_type": "doc",
            "_id": data['id_tx'],
            "_source": {
                "type": "streaming",
                "time": {'path': data['date'], 'format': TIME_FORMAT},
                "value": data['value']/BITCOIN_TO_SATOSHI,
                "addr": data["addr"]
            }
        }
        for data in dataset
    ]
    helpers.bulk(connections.get_connection(), actions)

def consume_Tx_Index(config,topic,index,master="local[2]", appName="CurrentTransaction", kafkaConsumer_host="localhost",kafkaConsumer_port=2181):
    """ Get the current information of transaction by a KafkaProducer and SparkStreaming/KafkaUtils, theses informations are send to an ElasticSearchBase.

    Arguments:
        topic {string} -- Name of topic to Consume in Kafka
        index {string} -- Name of index of ElasticSearchDataBase
        master {string} -- Set master URL to connect to
        appName {string} -- Set SparkApplication name
        kafkaConsumer_host {string} -- kafkaStream consumer to connect for getting streamingPrice
        kafkaConsumer_port {int} -- kafkaStream port to connect for

    Returns:
        void -- Send to ElasticSearchDataBase data.
    """
    sc = SparkContext(master, appName)
    strc = StreamingContext(sc, 60)

    millis=int(round(time.time() * 1000))
    kafkaStream = KafkaUtils.createStream(strc, kafkaConsumer_host+':'+str(kafkaConsumer_port), 'SparkStreaming', {topic: 1},valueDecoder=lambda m:convert(m))#.pprint()#,kafkaParams = {"metadata.broker.list": 'localhost:9092'})\
    #kafkaStream.pprint()
    #print("la")
    filtre=kafkaStream.map(lambda block:filtre_miner(json.loads(block[1])['x']['hash']))
    elastic=filtre.foreachRDD(lambda rdd: send(rdd, config))
    #print("time kafkastream est:")
    #print(int(round(time.time() * 1000))-millis)

    #kafkaStream.foreachRDD(lambda rdd: add_real_time_tx(index,rdd.collect()))

    strc.start()
    strc.awaitTermination()

if __name__ == "__main__":
    from config import config
    #connections.create_connection(hosts='localhost')#, http_auth=http_auth('elastic'))
    consume_Tx_Index(config,'mineur_str','bitcoin_mineur')
