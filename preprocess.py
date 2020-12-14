import joblib
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import os
import random

import time
# import atexit
# from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
# from apscheduler.schedulers.background import BackgroundScheduler


new_data = pd.read_csv('data/new_data_okla.csv')


one_voltage_power_consumed = 26962.15783860644
one_voltage_ride_duration = pd.to_timedelta('00:01:20.945609669')


def filter_data():
	distance_covered = []
	dropped_battery_voltage = []
	trip_id = []
	avg_gps_speed = []
	available_battery_voltage = []
	power_consumed = []
	avg_wheel_rpm = []
	trip_duration = []

	 # analyze by trip_id
	for id in random.sample(new_data.tripId.value_counts().index.to_list(), 1): 
		# min battery Voltage
		min_battery_voltage = new_data['underVoltageLimit'].unique().min()

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
		available_battery_voltage.append(trip_one['batteryVoltage'].min() -  min_battery_voltage)

		# distance travel from the starting of the trip to end
		distance_covered.append(round(trip_one['odometer'].max() - trip_one['odometer'].min(),2))

		# battery voltage utilize during the trip
		dropped_battery_voltage.append(trip_one['batteryVoltage'].max() - trip_one['batteryVoltage'].min())

		# power consumed during the trip
		power_consumed.append(round(trip_one['power_consumption'].sum()))

		# Average wheelrmp during the trip
		avg_wheel_rpm.append(round(trip_one['wheelRpm'].mean()))


		final_df = pd.DataFrame({'tripId': trip_id,'tripDuration': trip_duration, 'powerConsumed': power_consumed,
		                   'AvgWheelRPM': avg_wheel_rpm, 'batteryVoltageUsed':dropped_battery_voltage, 
		                   'DistanceCovered':distance_covered,'availableBatteryVoltage': available_battery_voltage,
		                   })
	# Calculate possible distance a rider can cover with the remaining battery
	final_df['possibleRideAvailable'] = round(final_df['availableBatteryVoltage'] * (final_df['DistanceCovered'] /final_df['batteryVoltageUsed']))

	# calculate the posible duration it can give
	final_df['possibleRideDuration'] = final_df['availableBatteryVoltage'].apply(lambda x : str(one_voltage_ride_duration * x).split(' ')[-1])

	# calculate the possible power battery will give
	final_df['possiblePowerAvailable'] = final_df['availableBatteryVoltage'].apply(lambda x : x * one_voltage_power_consumed)

	# drop column if battery voltage is 0
	final_df = final_df[~(final_df['possibleRideAvailable'] == np.inf)]

	# drop all those ride that have cover zero distance
	final_df.dropna(inplace=True)

	minutee = pd.to_datetime(final_df['tripDuration'].values).hour * 60 + pd.to_datetime(final_df['tripDuration'].values).minute + pd.to_datetime(final_df['tripDuration'].values).second / 60

	final_df['hr_sin'] = np.sin(minutee *(2.*np.pi/60))
	final_df['hr_cos'] = np.cos(minutee *(2.*np.pi/60))


	return final_df[['hr_sin', 'hr_cos', 'possiblePowerAvailable', 'AvgWheelRPM','availableBatteryVoltage']]
