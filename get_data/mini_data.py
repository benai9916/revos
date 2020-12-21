
import sys
import schedule
import time
import json
# from .static import preprocess
# from ..config import *

sys.path.insert(1, './static/preprocess')
sys.path.insert(1, './')

from config import ES_INDEX, ES_URL
from static.preprocess import *

print(ES_INDEX)

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

    # create a client instance of the library
    print ("\ncreating client instance of Elasticsearch")

    elastic_client = Elasticsearch(ES_URL)

    """
    MAKE API CALL TO CLUSTER AND CONVERT
    THE RESPONSE OBJECT TO A LIST OF
    ELASTICSEARCH DOCUMENTS
    """
    # total num of Elasticsearch documents to get with API call
    total_docs = 500

    search_body_v = {
        "query": {
            "bool": {
                "must": [
                    {"match": {'vin': 'OKLA_OFFICE_WHITE'}},
                    {"match": {'type': 1}},
                ]
            }
        }
    }

    print ("\nmaking API call to Elasticsearch for", total_docs, "documents.")

    response = elastic_client.search(
        index=ES_INDEX,
        body= search_body_v,
        size=total_docs,
        scroll='1m',
         _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
            'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit', 'gps_speed']   
    )

    # print(response['_scroll_id'])
    scroll_id = response['_scroll_id']


    # grab list of docs from nested dictionary response
    # print ("putting documents in a list")
    elastic_docs = response["hits"]["hits"]

    # print(type(elastic_docs))

    """
    GET ALL OF THE ELASTICSEARCH
    INDEX'S FIELDS FROM _SOURCE
    """
    #  create an empty Pandas DataFrame object for docs
    docs = pandas.DataFrame()

    # iterate each Elasticsearch doc in list
    print ("\ncreating objects from Elasticsearch data.")
    for num, doc in enumerate(elastic_docs):

        # get _source data dict from document
        source_data = doc["_source"]

        # get _id from document
        _id = doc["_id"]

        # create a Series object from doc dict object
        doc_data = pandas.Series(source_data, name = _id)

        # append the Series object to the DataFrame object
        docs = docs.append(doc_data)

    # pre_process the data
    pre_process(docs)

    print ("\n\ntime elapsed:", time.time()-start_time)


schedule.every(5).seconds.do(pull_elastic_data)

while True:
    schedule.run_pending()
    time.sleep(1)

# print ("\nexporting Pandas objects to different file types.")

# export Elasticsearch documents to a CSV file
# docs.to_csv("exp.csv", ",") # CSV delimited by commas

# export Elasticsearch documents to CSV
# csv_export = docs.to_csv(sep=",", index=False) # CSV delimited by commas
# print ("\nCSV data:", csv_export)