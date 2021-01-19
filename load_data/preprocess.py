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

from calculate_soc import *


# load_data = pd.read_csv('data/okla_office_white _new.csv')

min_battey_vol =  47
max_battery_vol = 72

def pre_process(df):
	df['time'] = df['timestamp'].apply(lambda x : x.split('T')[1][0:-1])
	df['date'] = df['timestamp'].apply(lambda x : x.split('T')[0])

	# drop unused columns
	df = df.drop(['chargeDischargeTimes', 'timestamp', 'ridingCurrent', 'totalMilage'], axis=1)

	# dropping all rows with missing tripId, gpsSpeed
	new_df_with_tripid = df.dropna(subset=['tripId', 'gpsSpeed'])

	# change tripId value
	# new_df_with_tripid['tripId'] = pd.factorize(new_df_with_tripid['tripId'])[0]

	# substract battery voltage with the minimum battery voltage
	# new_df_with_tripid['batteryVoltageAdc'] = new_df_with_tripid['batteryVoltageAdc'].apply(lambda x : x - 46)

	filter_data(new_df_with_tripid)


active_vin = []
def filter_data(new_data):
	# global active_vin
	
	print('ths is vin ->', (active_vin))
	trip_id = []
	trip_duration = []
	distance_covered = []
	dropped_battery_voltage = []
	available_battery_voltage = []
	avg_gps_speed = []
	initial_battery_voltage = []
	vin = []

	if len(active_vin) != 0:
		new_data =  new_data.loc[new_data['vin'] == active_vin[-1]]

	single_trip_id = random.sample(new_data['tripId'].value_counts().index.to_list(), 1)[0]

	if 0 not in new_data.loc[new_data['tripId'] == single_trip_id]['ignition'].unique():

		# analyze by trip_id
		# for id in trip_id:
			# sort trip id
		trip_one = new_data.loc[new_data['tripId'] == single_trip_id].sort_values(by='time', ascending=True)

		print('this is the shape', trip_one.shape)

		vin.append(trip_one['vin'].values[0])
		
		# trip id
		trip_id.append(single_trip_id)

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
								'droppedBatteryVoltage': dropped_battery_voltage, 'DistanceCovered':distance_covered})


		# function to calculate soc
		calculate_soc(final_df1)

		final_df1['availableBatteryVoltage'] = final_df1['availableBatteryVoltage'].apply(lambda x : x - 46)


		final_df1['vin'] = vin

		if len(active_vin) != 0 and final_df1['vin'].values[0] in active_vin:
			pass
		else:
			active_vin.append(final_df1['vin'].values[0])


		# Calculate possible distance a rider can cover with the remaining battery
		final_df1['possibleRideAvailable'] = final_df1['availableBatteryVoltage'] * (final_df1['DistanceCovered'] / final_df1['droppedBatteryVoltage'])

		# arguments for the model
		final_df = final_df1[['availableBatteryVoltage','droppedBatteryVoltage', 'DistanceCovered','avgGpsSpeed']]

		# args = {'data': final_df.values.tolist()}

		send_data = {'availableBatteryVoltage': final_df.values[0][0],
			 'droppedBatteryVoltage':  final_df.values[0][1],
			 'DistanceCovered' : final_df.values[0][2] ,
			 'avgGpsSpeed' : final_df.values[0][3]
			}


		# print('------------',send_data)

		if final_df['droppedBatteryVoltage'].values[0] >= 0.01 and final_df['DistanceCovered'].values[0] != 0:
			res = requests.get('http://15.206.179.38/range?', params=send_data)
			# res = requests.get('http://127.0.0.1:5000/range?', params=send_data)

		# requests.get('http://15.206.179.38/?data='+send_data)

			print('Data for vim --> ', active_vin[-1], res.json())

	else:
		active_vin.clear()
		pass

# pre_process(load_data)

# schedule.every(5).seconds.do(pre_process, load_data)

# while True:
#     schedule.run_pending()
#     time.sleep(1)