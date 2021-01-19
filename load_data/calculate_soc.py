
import json
import requests


def calculate_soc(final_df):
	send_data = final_df[['availableBatteryVoltage','droppedBatteryVoltage', 'DistanceCovered','avgGpsSpeed']]

	# new_soc = json.dumps(send_data.values.tolist())
	new_soc = {'availableBatteryVoltage': send_data.values[0][0],
			 'droppedBatteryVoltage':  send_data.values[0][1],
			 'DistanceCovered' : send_data.values[0][2] ,
			 'avgGpsSpeed' : send_data.values[0][3]
			}

	# make api call
	# res = requests.get('http://127.0.0.1:5000/soc?', params = new_soc)
	res = requests.get('http://15.206.179.38/soc?', params = new_soc)
	
	print('SOC value -->',res.json())