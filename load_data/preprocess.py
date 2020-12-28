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

MIN_VOLTAGE = 46


def pre_process(df):
	print(df.shape)
	df['time'] = df['timestamp'].apply(lambda x : x.split('T')[1][0:-1])
	df['date'] = df['timestamp'].apply(lambda x : x.split('T')[0])

	# drop unused columns
	df = df.drop(['chargeDischargeTimes', 'timestamp', 'ridingCurrent', 'totalMilage'], axis=1)

	# dropping all rows with missing tripId, gpsSpeed
	new_df_with_tripid = df.dropna(subset=['tripId', 'gpsSpeed'])

	# change tripId value
	# new_df_with_tripid['tripId'] = pd.factorize(new_df_with_tripid['tripId'])[0]

	# substract battery voltage with the minimum battery voltage
	new_df_with_tripid['batteryVoltageAdc'] = new_df_with_tripid['batteryVoltageAdc'].apply(lambda x : x - 46)


	filter_data(new_df_with_tripid)



def filter_data(new_data):
	trip_id = []
	trip_duration = []
	distance_covered = []
	dropped_battery_voltage = []
	available_battery_voltage = []
	avg_gps_speed = []
	initial_battery_voltage = []
	
	# analyze by trip_id
	for id in random.sample(new_data.tripId.value_counts().index.to_list(), 1):
		# sort trip id
		trip_one = new_data.loc[new_data['tripId'] == id].sort_values(by='time', ascending=True)
		
		# trip id
		trip_id.append(id)

		# trip duration 
		# trip start and end time
		trip_s = trip_one.loc[trip_one.head(1).index, (['time'])].values[0][0]
		trip_e = trip_one.loc[trip_one.tail(1).index, (['time'])].values[0][0]
		start = pd.to_datetime(trip_s)
		end = pd.to_datetime(trip_e)
		trip_duration.append(str(timedelta(minutes = pd.Timedelta(end - start).seconds / 60.0)))

		# initial battery voltage
		initial_battery_voltage.append(trip_one['batteryVoltageAdc'].max())

		# availabe battery voltage
		available_battery_voltage.append(trip_one['batteryVoltageAdc'].min())

		# distance travel from the starting of the trip to end
		if trip_one['latitude'].head(1).values[0] != 0 and trip_one['latitude'].tail(1).values[0] != 0:
			coords_1 = (trip_one['latitude'].head(1).values[0], trip_one['longitude'].head(1).values[0])
			coords_2 = (trip_one['latitude'].tail(1).values[0], trip_one['longitude'].tail(1).values[0])

			distance_covered.append(geopy.distance.geodesic(coords_1, coords_2).km)
		else:
			distance_covered.append(0)

		# battery voltage utilize during the trip
		dropped_battery_voltage.append(trip_one['batteryVoltageAdc'].max() - trip_one['batteryVoltageAdc'].tail(1).values[0])
		
		# average speed during the ride
		avg_gps_speed.append(trip_one['gpsSpeed'].mean())


		final_df1 = pd.DataFrame({'tripId': trip_id, 'tripDuration': trip_duration,
								'BatteryVoltageOnIgnition': initial_battery_voltage, 'availableBatteryVoltage': available_battery_voltage, 'avgGpsSpeed': avg_gps_speed,
								'batteryVoltageUsed':dropped_battery_voltage, 'DistanceCovered':distance_covered})


	# Calculate possible distance a rider can cover with the remaining battery
	final_df1['possibleRideAvailable'] = final_df1['availableBatteryVoltage'] * (final_df1['DistanceCovered'] / final_df1['batteryVoltageUsed'])

	# arguments for the model
	final_df = final_df1[['availableBatteryVoltage','batteryVoltageUsed', 'DistanceCovered','avgGpsSpeed']]

	# args = {'data': final_df.values.tolist()}

	send_data = json.dumps(final_df.values.tolist())

	print('------------',send_data)

	if final_df['batteryVoltageUsed'].values[0] >= 0.01:
	# res = requests.get('http://15.206.179.38/range?data='+send_data)
		res = requests.get('http://127.0.0.1:5000/range?data='+send_data)
	

		# requests.get('http://15.206.179.38/?data='+send_data)

		print('>>>>>>>>>>>>>>>>>>>>>..', res.json())

		return res.json()

# pre_process(load_data)

# schedule.every(5).seconds.do(pre_process, load_data)

# while True:
#     schedule.run_pending()
#     time.sleep(1)