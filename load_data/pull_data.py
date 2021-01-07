
import sys
import schedule
import time
import json

sys.path.insert(1, './')

from config import ES_INDEX, ES_URL
from preprocess import *


start_time = time.time()

def pull_elastic_data():

    if sys.version[0] != "3":
        print ("\nThis script requires Python 3")
        print ("Please run the script using the 'python3' command.\n")
        quit()

    try:
        # import the Elasticsearch low-level client library
        from elasticsearch import Elasticsearch

        # import Pandas, JSON, and the NumPy library
        import pandas, json
        import numpy as np

    except ImportError as error:
        print ("\nImportError:", error)
        print ("Please use 'pip3' to install the necessary packages.")
        quit()


    elastic_client = Elasticsearch(ES_URL)

    """
    MAKE API CALL TO CLUSTER AND CONVERT
    THE RESPONSE OBJECT TO A LIST OF
    ELASTICSEARCH DOCUMENTS
    """
    # total num of Elasticsearch documents to get with API call
    total_docs = 3000
    active_vin = []
    # vim = 'M22YCESD20B000705'

    search_body_v = {
        "query": {
            "bool": {
                "must": [
                    # {"match": {'vin': vim}},
                    {"match": {'type': 'ANALYTICS'}}
                ]
            }
        },
        "sort": { "timestamp": "desc"},
    }

    print ("\nmaking API call to Elasticsearch for", total_docs, "documents.")

    response = elastic_client.search(
        index=ES_INDEX,
        body= search_body_v,
        size=total_docs,
        scroll='1m',
         _source= [
        'tripId',
        'vin', 
        'timestamp', 
        'relativeSOC', 
        'batteryVoltageAdc', 
        'gpsSpeed', 
        'ignition', 
        'latitude', 
        'longitude', 
        'ridingCurrent',
        'chargeDischargeTimes', 
        'totalMilage', 'type']   
    )

    # print(response['_scroll_id'])
    scroll_id = response['_scroll_id']


    # grab list of docs from nested dictionary response
    # print ("putting documents in a list")
    elastic_docs = response["hits"]["hits"]

    # print((elastic_docs))

    """
    GET ALL OF THE ELASTICSEARCH
    INDEX'S FIELDS FROM _SOURCE
    """
    #  create an empty Pandas DataFrame object for docs
    docs = pandas.DataFrame()

    # iterate each Elasticsearch doc in list
    for num, doc in enumerate(elastic_docs):

        # get _source data dict from document
        source_data = doc["_source"]

        # print((source_data))

        # get _id from document
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pandas.Series(source_data, name = _id)

        # append the Series object to the DataFrame object
        docs = docs.append(doc_data)

    # pre_process the data
    pre_process(docs)
    # print('It is running..........................')

    print ("\n\ntime elapsed:", time.time()-start_time)


# pull_elastic_data()
schedule.every(2).minutes.do(pull_elastic_data)

while True:
    schedule.run_pending()
    time.sleep(1)

# print ("\nexporting Pandas objects to different file types.")

# export Elasticsearch documents to a CSV file
# docs.to_csv("exp.csv", ",") # CSV delimited by commas

# export Elasticsearch documents to CSV
# csv_export = docs.to_csv(sep=",", index=False) # CSV delimited by commas
# print ("\nCSV data:", csv_export)