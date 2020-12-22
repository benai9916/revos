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


one_voltage_power_consumed = 2971.5567
# one_voltage_ride_duration = pd.to_timedelta('00:01:09.571291076')

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

	if (len(send_data) > 2) and  (final_df['batteryVoltageUsed'].values[0] >= 0.3):
		res = requests.get('http://15.206.179.38/range?data='+send_data)

		# requests.get('http://15.206.179.38/?data='+send_data)

		print('>>>>>>>>>>>>>>>>>>>>>..', res.json())

		return res.json()

# pre_process(load_data)

# schedule.every(5).seconds.do(pre_process, load_data)

# while True:
#     schedule.run_pending()
#     time.sleep(1)