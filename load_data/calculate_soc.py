
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

	# new_soc = json.dumps(send_data.values.tolist())
	new_soc = {'availableBatteryVoltage': send_data.values[0][0],
			 'droppedBatteryVoltage':  send_data.values[0][1],
			 'DistanceCovered' : send_data.values[0][2] ,
			 'avgGpsSpeed' : send_data.values[0][3],
			 'new_soc' : send_data.values[0][4]
			}

	# make api call
	# res = requests.get('http://127.0.0.1:5000/soc?', params = new_soc)
	res = requests.get('http://15.206.179.38/soc?', params = new_soc)
	
	print(' ============>>>>>>>>>',res.json())