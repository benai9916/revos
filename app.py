from flask import Flask, request, jsonify, render_template
import os
from flask_cors import CORS 
import joblib
import json
import numpy as np

# ml libraries
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_squared_log_error, r2_score, explained_variance_score


import time

app = Flask(__name__)
cors = CORS(app)

app.config['DEBUG'] = True

distances = []
arguments = []
def model(parms):
	global  arguments
	global  distance

	okla_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/cb_combine.sav'))

	# parms = [i for i in json.loads((parms[0]))[0]]


	ranges = okla_catBoost_model.predict(np.array([parms]))

	distance = {'range': abs(round(ranges.tolist()[0],2))}

	arguments.append(parms)
	distances.append(distance)

	return distance


@app.route('/range', methods=['GET', "POST"])
def calculate_range():
	four_parms = []
	a = request.args.to_dict()

	for i in a.keys():
		four_parms.append(a[i])
		# print('--==', four_parms)

	total_range = model(four_parms)

	print('----------', total_range)

	return total_range
	# return render_template('index.html', range = abs(distance), four_parms=arguments)


@app.route('/')
def home():
	if len(distances) != 0:
		return render_template('index.html', range= distances[0], four_parms=arguments[0])
	else:
		return render_template('index.html', error='Refresh after few minute')



new_soc_val = []
min_battey_vol =  47
max_battery_vol = 72
stabalize_value = 2
@app.route('/soc', methods=['GET', "POST"])
def calculate_soc():
	global new_soc_val
	new_soc_val.clear()

	all_params = request.args.to_dict()

	for i in all_params.keys():
		new_soc_val.append(all_params[i])

	# calculate soc
	new_voltage = float(all_params['availableBatteryVoltage']) + stabalize_value

	final_soc = (new_voltage - min_battey_vol) / (max_battery_vol - min_battey_vol) * 100 

	# append the soc
	new_soc_val.append(final_soc)
	
	print('===============', final_soc)

	return {'new_soc': abs(final_soc)}


@app.route('/soc_val')
def dsiaply_soc():
	return render_template('soc.html', soc_parms = abs(new_soc_val))



if __name__ == '__main__':
    app.run()