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



''' ------------------------------------okla office  --------------------------------------------------
import joblib
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import os
import random
import requests
import geopy.distance

import schedule
import time
import json

# load_data = pd.read_csv('data/okla_office_white _new.csv')


def adjust_battery_voltage(volt, under_battery_vol):
    return volt - under_battery_vol 


def pre_process(df):
    # drop any missing values
    new_data = df.dropna()

    # select columns
    new_data = new_data[['tripId', 'type', 'latitude', 'longitude','batteryVoltage',
     'batteryCurrent', 'wheelRpm', 'throttle', 'timestamp', 'underVoltageLimit', 'overVoltageLimit']]

    # over_battery_vol = new_data['overVoltageLimit'].mean()
    under_battery_vol = new_data['underVoltageLimit'].mean()

    # change the timestampe with datetime formate
    new_data['timestamp'] = new_data['timestamp'].apply(lambda x : datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))

    # crete new date and time column
    new_data['date'] = [d.split(' ')[0] for d in new_data['timestamp']]
    new_data['time'] = [d.split(' ')[1] for d in new_data['timestamp']]

    # calculate the under and over battery voltage
    under_battery_vol = new_data['underVoltageLimit'].mode().values[0]
    over_battery_vol = new_data['overVoltageLimit'].mode().values[0]
    new_data['batteryVoltage'] = new_data['batteryVoltage'].apply(lambda x : adjust_battery_voltage(x, under_battery_vol) )


    # print(new_data.head(1))

    filter_data(new_data)



def filter_data(new_data):
    distance_covered = []
    dropped_battery_voltage = []
    trip_id = []
    avg_gps_speed = []
    available_battery_voltage = []
    power_consumed = []
    avg_wheel_rpm = []
    trip_duration = []
    avg_throttle = []

     # analyze by trip_id
    for id in random.sample(new_data.tripId.value_counts().index.to_list(), 1):
        # min battery Voltage
        # min_battery_voltage = new_data['underVoltageLimit'].unique().min()

        # inject each trip_id
        trip_one = new_data.loc[new_data['tripId'] == id].sort_values(by='time', ascending=True)

        # calculate total power consumption
        trip_one['power_consumption'] = trip_one['batteryVoltage'] * trip_one['batteryCurrent']

        # trip start and end time
        trip_s = trip_one.loc[trip_one.head(1).index, (['time'])].values[0][0]
        trip_e = trip_one.loc[trip_one.tail(1).index, (['time'])].values[0][0]

        # trip duration 
        start = pd.to_datetime(trip_s)
        end = pd.to_datetime(trip_e)
        trip_duration.append(str(timedelta(minutes = pd.Timedelta(end - start).seconds / 60.0)))

        # trip id
        trip_id.append(id)

        # availabe battery voltage
        available_battery_voltage.append(trip_one['batteryVoltage'].min())

        # distance travel from the starting of the trip to end
        # distance_covered.append(round(trip_one['odometer'].max() - trip_one['odometer'].min(),2))
        if trip_one['latitude'].head(1).values[0] != 0 and trip_one['latitude'].tail(1).values[0] != 0:
            coords_1 = (trip_one['latitude'].head(1).values[0], trip_one['longitude'].head(1).values[0])
            coords_2 = (trip_one['latitude'].tail(1).values[0], trip_one['longitude'].tail(1).values[0])

            distance_covered.append(geopy.distance.geodesic(coords_1, coords_2).km)
        else:
            distance_covered.append(0)

        # battery voltage utilize during the trip
        dropped_battery_voltage.append(trip_one['batteryVoltage'].max() - trip_one['batteryVoltage'].min())

        # power consumed during the trip
        power_consumed.append(round(trip_one['power_consumption'].sum(), 2))

        # Average wheelrmp during the trip
        avg_wheel_rpm.append(round(trip_one['wheelRpm'].mean(), 2))

        # Average throtle during the trip
        avg_throttle.append(round(trip_one['throttle'].mean(), 2))


        final_df = pd.DataFrame({'tripId': trip_id,'tripDuration': trip_duration, 'powerConsumed': power_consumed,
                           'AvgWheelRPM': avg_wheel_rpm, 'batteryVoltageUsed':dropped_battery_voltage, 'AvgThrottle': avg_throttle,
                           'DistanceCovered':distance_covered,'availableBatteryVoltage': available_battery_voltage,
                           })

    # Calculate possible distance a rider can cover with the remaining battery
    # final_df['possibleRideAvailable'] = final_df['availableBatteryVoltage'] * (final_df['DistanceCovered'] /final_df['batteryVoltageUsed'])

    # calculate the posible duration it can give
    # final_df['possibleRideDuration'] = final_df['availableBatteryVoltage'].apply(lambda x : str(one_voltage_ride_duration * x).split(' ')[-1])

    # calculate the possible power battery will give
    if final_df['batteryVoltageUsed'].sum() == 0:
        # mean_power_consumed = final_df['powerConsumed'].mean()
        # mean_voltage_used = final_df['batteryVoltageUsed'].mean()
        # one_voltage_power_consumed = round(mean_power_consumed / mean_voltage_used, 2)
        final_df['possiblePowerAvailable'] = final_df['availableBatteryVoltage'].apply(lambda x : x * one_voltage_power_consumed)
    else:
        final_df['possiblePowerAvailable'] = final_df['availableBatteryVoltage'] * (final_df['powerConsumed'] / final_df['batteryVoltageUsed'])
    
    # if value is inf then replace with 0
    # final_df['possibleRideAvailable'] = final_df[['possibleRideAvailable']].replace(np.inf, 0)

    # drop all those ride that have cover zero distance
    final_df.dropna(inplace=True)

    # minutee = pd.to_datetime(final_df['tripDuration'].values).hour * 60 + pd.to_datetime(final_df['tripDuration'].values).minute + pd.to_datetime(final_df['tripDuration'].values).second / 60

    # final_df['hr_sin'] = np.sin(minutee *(2.*np.pi/60))
    # final_df['hr_cos'] = np.cos(minutee *(2.*np.pi/60))

    final_df = final_df[['batteryVoltageUsed', 'powerConsumed', 'AvgWheelRPM', 'AvgThrottle']]

    # args = {'data': final_df.values.tolist()}

    send_data = json.dumps(final_df.values.tolist())

    print('------------',send_data)

    if (len(send_data) > 2) and  (final_df['batteryVoltageUsed'].values[0] >= 0.2):
        res = requests.get('http://15.206.179.38/range?data='+send_data)

        # requests.get('http://15.206.179.38/?data='+send_data)

        print('>>>>>>>>>>>>>>>>>>>>>..', res.json())

        return res.json()

# pre_process(load_data)

# schedule.every(5).seconds.do(pre_process, load_data)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

-----------------------------------------------END ----------------------------------------------- '''