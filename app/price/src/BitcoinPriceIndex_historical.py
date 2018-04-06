import json
import datetime
import time
import logging

from http import client as httpClient
from http import HTTPStatus

from elasticsearch_dsl.connections import connections
from elasticsearch import helpers

from elastic_storage import storeData, eraseData, BitCoin, http_auth, connectionToAPI

DEFAULT_HOST = "api.coindesk.com"
DEFAULT_URI_DATE = "/v1/bpi/historical/close.json?currency=EUR"
HOUR_SECONDS = 3600

def getHistoricalPrice(start, end, host=DEFAULT_HOST, path=DEFAULT_URI_DATE):
    """ Call the API to get all the bitcoin values between two dates
    
    Arguments:
        start {string} -- [description]
        end {string} -- [description]
    
    Keyword Arguments:
        host {string} -- [description] (default: {DEFAULT_HOST})
        path {string} -- [description] (default: {DEFAULT_URI_DATE})
    
    Returns:
        json -- [description]
    """

    return connectionToAPI(host, path+"&start="+start+"&end="+end)

def createHistoricalDataset(jsonData):
    """ Creates a list from the json data
    
    Arguments:
        jsonData {json} -- [description]
    
    Returns:
        list -- [description]
    """

    list = []
    if jsonData:
        for key, val in jsonData['bpi'].items():
            tempDic = {}
            tempDic['date'] = key+"T09:00:00"
            tempDic['value'] = val
            list.append(tempDic)
    return list

def addHistoricalDataset(start, end):
    """ Add data from the API between two dates to Elastic
    
    Arguments:
        start {string} -- [description]
        end {string} -- [description]
    """

    try:
        eraseData("historical", ind="bitcoin_price")
    except:
        logging.info("no data to erase!")
    jsonDataH = getHistoricalPrice(start, end)
    historicalDataset = createHistoricalDataset(jsonDataH)
    if historicalDataset:
        ''' Call to bulk api to store the data '''
        actions = [
            {
                "_index": "bitcoin_price",
                "_type": "doc",
                "date": data['date'],
                "value": data['value'],
                "type": "historical"
            }
            for data in historicalDataset
        ]
        helpers.bulk(connections.get_connection(), actions)

def insertHistoricalDataInBase():
    ''' Puts the historical data into elasticsearch '''
    addHistoricalDataset("2010-07-17", str(datetime.date.today()))

if __name__ == "__main__":
    from config import config
    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth(config['elasticsearch']))
    insertHistoricalDataInBase()
    while True: 
        time.sleep(HOUR_SECONDS)
        if datetime.datetime.now().hour == 2:
            today = str(datetime.date.today())
            addHistoricalDataset(today, today)
            logging.info("INFO")
            eraseData("real-time")
