# import schedule
# import time
# import pandas as pd
# import numpy as np


# from flask import Flask
# from flask_apscheduler import APScheduler

# app = Flask(__name__)

# class Config(object):
#     SCHEDULER_API_ENABLED = True


# scheduler = APScheduler()


# new_data = pd.read_csv('data/new_data_okla.csv')


# interval examples
# @scheduler.task('interval', id='do_job_1', seconds=3, misfire_grace_time=900)
# def job1():
#     print('Job 1 executed')


# # cron examples
# @scheduler.task('cron', id='do_job_2', minute='*')
# def job2():
#     print('Job 2 executed')


# @scheduler.task('cron', id='do_job_3', week='*', day_of_week='mon')
# def job3():
#     print('Job 3 executed')


# @scheduler.task('interval', id='do_job_1', seconds=40, misfire_grace_time=900)
# @app.route('/')
# def home():
# 	data = test()
# 	print(data)
# 	return str(data)



# call the data every 30 second
# @scheduler.task('interval', id='do_job_1', seconds=20, misfire_grace_time=900)
# def data():
# 	global range_arguments
# 	range_arguments = filter_data()
# 	print('--- data',range_arguments)
# 	return None


# @scheduler.task('interval', id='do_job_2', seconds=30, misfire_grace_time=900)
# @app.route('/')
# def home():
# 	aima_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/okla_catboost.sav'))

# 	ride_range = aima_catBoost_model.predict(np.array(range_arguments))

# 	print('--- -------------------------------- Range -----------------------')
# 	print(ride_range)
# 	print('--- -------------------------------- -----------------------',ride_range)

# 	distance = {'arguments': [list(i) for i in range_arguments.values],'range': round(ride_range.tolist()[0],2)}

# 	return distance

# from apscheduler.schedulers.blocking import BlockingScheduler
# import requests

# def some_job():
# 	res = requests.get('http://15.206.179.38/range?data=[[12,2,3,4,5]]')
# 	print(res.json())

# scheduler = BlockingScheduler()
# scheduler.add_job(some_job, 'interval', seconds=2)
# scheduler.start()




#!/usr/bin/env python3
#-*- coding: utf-8 -*-

# import the Elasticsearch client library
from elasticsearch import Elasticsearch, exceptions

# import JSON and time
import json, time, pandas

# create a timestamp using the time() method
start_time = time.time()

# declare globals for the Elasticsearch client host
# concatenate a string for the client's host paramater


# declare an instance of the Elasticsearch library
client = Elasticsearch("https://search-rev-elastic-pavbwqniud3xfj2ufsvvrkiive.ap-south-1.es.amazonaws.com")

try:
    # use the JSON library's dump() method for indentation
    info = json.dumps(client.info(), indent=4)

    # pass client object to info() method
    print ("Elasticsearch client info():", info)

except exceptions.ConnectionError as err:

    # print ConnectionError for Elasticsearch
    print ("\nElasticsearch info() ERROR:", err)
    print ("\nThe client host:", host, "is invalid or cluster is not running")

    # change the client's value to 'None' if ConnectionError
    client = None

# valid client instance for Elasticsearch
if client != None:

    # get all of the indices on the Elasticsearch cluster
    # all_indices = client.indices.get_alias("*")

    # keep track of the number of the documents returned
    doc_count = 0

    # iterate over the list of Elasticsearch indices
    # for num, index in enumerate(all_indices):

        # declare a filter query dict object
    match_all = {
        "size": 2,
        "query": {
            'match': {
        		'vin': 'OKLA_OFFICE_WHITE'
        	}
        }
    }

    # make a search() request to get all docs in the index
    resp = client.search(
        index = 'events-rev-es-vehicle000',
        body = match_all,
        scroll = '60s', # length of time to keep search context
        _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
                'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit']
    )

    print('---------------', len(resp['hits']['hits']))
    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']

    # use a 'while' iterator to loop over document 'hits'
    docs = pandas.DataFrame()

    while len(resp['hits']['hits']):

        # make a request using the Scroll API
        resp = client.scroll(
            scroll_id = old_scroll_id,
            scroll = '40s' # length of time to keep search context
        )

        # check if there's a new scroll ID
        if old_scroll_id != resp['_scroll_id']:
            print ("NEW SCROLL ID:", resp['_scroll_id'])

        # keep track of pass scroll _id
        old_scroll_id = resp['_scroll_id']

        # print the response results++
        # print ("\nresponse for index:", index)
        # print ("_scroll_id:", resp['_scroll_id'])
        # print ('response["hits"]["total"]["value"]:', resp["hits"],["total"],["value"])

        # iterate over the document hits for each 'scroll'

        for no, doc in enumerate(resp['hits']['hits']):
            print ("\n",no, doc['_id'], doc['_source'])
            doc_count += 1
            print ("DOC COUNT:", doc_count)

            source_data = doc["_source"]

            _id = doc["_id"]

            doc_data = pandas.Series(source_data, name = _id)

            docs = docs.append(doc_data)

    # print the total time and document count at the end
    print ("\nTOTAL DOC COUNT:", doc_count)

docs.to_csv("haha.csv", ",")

# print the elapsed time
print ("TOTAL TIME:", time.time() - start_time, "seconds.")