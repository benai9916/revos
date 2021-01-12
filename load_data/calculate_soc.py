
import json
import requests

min_battey_vol =  47
max_battery_vol = 72
stabalize_value = 2

def calculate_soc(final_df):
	print(final_df)
	final_df['availableBatteryVoltage'] = final_df['availableBatteryVoltage'] + stabalize_value

	final_df['new_soc'] = (final_df['availableBatteryVoltage'] - min_battey_vol) / (max_battery_vol - min_battey_vol) * 100 

	send_data = final_df[['availableBatteryVoltage','droppedBatteryVoltage', 'DistanceCovered','avgGpsSpeed', 'new_soc']]

	new_soc = json.dumps(send_data.values.tolist())

	# make api call
	# res = requests.get('http://127.0.0.1:5000/soc?data='+new_soc
	res = requests.get('http://15.206.179.38/soc?data='+new_soc)
	
	print(' ============>>>>>>>>>',res.json())