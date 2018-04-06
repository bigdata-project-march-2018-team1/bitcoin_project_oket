# -*- coding: utf-8 -*-
import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler

import json
import re
import numpy as np
import datetime
import time

from nltk.corpus import stopwords

from http import client as httpClient
from http import HTTPStatus

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline, PipelineModel
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.mllib.util import MLUtils

from elasticsearch_dsl import connections

from elastic_storage import getHistoricalPrice, createHistoricalDataset, http_auth

from config import config

access_token = '963341451273887744-fyNcKmcLd2HRYktyU3wVMshB4eYWMoh'
access_token_secret = 'fxOtX3rk3KXqiF50mFDEYTx19E3wNVMZeSIuXmozNxmHa'
consumer_key = 'bG58SBJQV8Hiqqjcu3jzXwfCL'
consumer_secret = 'kjcBffzpn9QYZsV91NZUqKhgGKBvehLyVfuvc0pm8Gh8sEPui8'

def connectionToTwitterAPI():
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return listener, auth

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream. 
    This is a basic listener that just prints received tweets to stdout.

    """

    def __init__(self):
        self.tweet = ""

    def on_data(self, data):
        self.tweet = json.loads(data)['text']
        return True

    def on_error(self, status):
        print(status)

def getTweets(auth, date_since):
    api = tweepy.API(auth)
    search_terms = "#bitcoin"
    for tweet in tweepy.Cursor(api.search,\
                        q=search_terms,\
                        since=date_since,\
                        lan="en",
                        count='10')\
                        .items():
        current_tweet = {}
        current_tweet['date'] = tweet.created_at
        current_tweet['text'] = tweet.text
        yield current_tweet

DEFAULT_HOST = "newsapi.org"

def getGoogleArticle(date,host=DEFAULT_HOST):
    connection = httpClient.HTTPConnection(host)
    current = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S") - datetime.timedelta(days=1)
    date_start = datetime.datetime.strftime(current, "%Y-%m-%dT%H:%M:%S")
    date_end = date
    uri="/v2/everything?q=bitcoin&from="+date_start+"&to="+date_end+"&pageSize=10&apiKey=3452795ae84242bd87160c899376718a"
    connection.request("GET", uri)
    resp = connection.getresponse()
    result = {}
    if resp.status == HTTPStatus.OK:
        result = json.loads(resp.read().decode('utf-8'))
    connection.close()
    return result

def cleaningText(tweet):
    tweet_lower = tweet.lower()
    tweet_nonAlpha = re.sub('[^a-zA-Z@# ]','',tweet_lower)
    tweet_split = tweet_nonAlpha.split(' ')
    tweet_not_doublons = set(tweet_split)
    tweet_clean = []
    stop_words = set(stopwords.words('english'))
    for word in tweet_not_doublons:
        if word not in stop_words:
            tweet_clean.append(word)
    return tweet_clean

'''def getVADERscores(tweets):
    scores_vader = []
    analyzer = SentimentIntensityAnalyzer()
    for tw in tweets:
        scores_vader.append(float(analyzer.polarity_scores(tw['contains'])['compound']))
    return scores_vader'''

def getResponseVariables(date):
    end = stringToDatetime(date) + datetime.timedelta(days=1)
    jsonDataH = getHistoricalPrice(date[0:10],end.strftime('%Y-%m-%d'))
    historicalDataset = createHistoricalDataset(jsonDataH)
    historicalDataset_sorted = sorted(historicalDataset, key=lambda k: k['date'])
    for i in range(len(historicalDataset_sorted)-1):
        diff = historicalDataset_sorted[i+1]['value'] - historicalDataset_sorted[i]['value']
        if diff <= 0:
            if abs(diff/historicalDataset_sorted[i]['value']) <= 0.1:
                Y = 0
            if abs(diff/historicalDataset_sorted[i]['value']) > 0.1:
                Y = 1
        if diff > 0:
            if abs(diff/historicalDataset_sorted[i]['value']) <= 0.1:
                Y = 2
            if abs(diff/historicalDataset_sorted[i]['value']) > 0.1:
                Y = 3
    return Y

def getCorpusPerDate(date):
    articles = getGoogleArticle(date)
    corpus = []
    date_publication = ()
    for art in articles['articles']:
        if art['description'] != None:
            description = art['description']
            date_publication = date_publication + (art['publishedAt'],)
            corpus = corpus + cleaningText(description)
    return corpus

def stringToDatetime(date):
    """ Convert a string date to a datetime date
    
    Arguments:
        date {string} -- Date in string format
    
    Returns:
        datetime -- Date in datetime format
    """

    return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")

def getCorpus_between_2_dates(start,end):
    corpus = []
    corpus.append([getCorpusPerDate(start),getResponseVariables(start),start])
    current_date = stringToDatetime(start)
    end_datetime = stringToDatetime(end)
    current_date += datetime.timedelta(days=1)
    while current_date < end_datetime:
        current_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
        corpus.append([getCorpusPerDate(current_date_str),
                       getResponseVariables(current_date_str), current_date_str])
        current_date += datetime.timedelta(days=1)
    return corpus


def getCorpus_custom(start):
    corpus = []
    corpus.append([getCorpusPerDate(start),
                   0, start])
    return corpus

def create_model():
    vectorizer = CountVectorizer(inputCol='text', outputCol="features")

    label_indexer = StringIndexer(inputCol="label", outputCol="label_index")

    classifier = NaiveBayes(
        labelCol="label_index", featuresCol="features", predictionCol="label_index_predicted")

    pipeline_model = Pipeline(stages=[vectorizer, label_indexer, classifier])

    return pipeline_model

def predict_today(sc,spark,pipeline_model):
    
    today = datetime.datetime.today()
    today_str = datetime.datetime.strftime(today, "%Y-%m-%dT%H:%M:%S")

    corpus_today = getCorpus_custom(today_str)

    rdd = sc.parallelize(corpus_today).map(lambda line: Row(text=line[0]))
    df = spark.createDataFrame(rdd)

    predict = pipeline_model.transform(df)

    return predict

def add_predict(df):
    pred = df.select(df['label_index_predicted']).rdd.collect()
    if pred:
        if pred[0] == 0:
            prediction="Down"
        if pred[0] == 1:
            prediction="Down10"
        if pred[0] == 2:
            prediction="Up"
        if pred[0] == 3:
            prediction="Up10"
        today = datetime.datetime.today()
        today_str = datetime.datetime.strftime(today, "%Y-%m-%dT%H:%M:%S")
        doc = { 'date': today_str, 'prediction': prediction }
        connections.get_connection().index(index="bitcoin_pred", doc_type="doc", _id=1, body=doc)

def main():
    sc = SparkContext()
    spark = SparkSession(sc)
    
    corpus = getCorpus_between_2_dates("2018-03-01T23:59:00", "2018-03-29T23:59:00")

    rdd = sc.parallelize(corpus).map(lambda v: Row(text=v[0],label=v[1],date=v[2]))
    df = spark.createDataFrame(rdd)

    df.show()

    df_train, df_test = df.randomSplit([0.6,0.4])

    pipeline_model = create_model()

    model = pipeline_model.fit(df_train)
    #model.write().overwrite().save("./model")

    predict = model.transform(df_test)

    evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="label_index_predicted", metricName="accuracy")
    accuracy = evaluator.evaluate(predict)
    #print("Accuracy: ", str(accuracy))

    #pipeline_model = PipelineModel.load("./model")

    predict = predict_today(sc,spark,model)
    predict.show()

    connections.create_connection(hosts=config['elasticsearch']['hosts'], http_auth=http_auth('elastic'))
    add_predict(predict)

if __name__ == "__main__":
    main()
    '''while True:
        main()
        time.sleep(84600)'''
