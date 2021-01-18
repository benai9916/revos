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

	distance = {'range': round(ranges.tolist()[0],2)}

	arguments.append(parms)
	distances.append(distance)

	return distance


@app.route('/range', methods=['GET', "POST"])
def calculate_range():
	four_parms = []
	a = request.args.to_dict()

	for i in a:
		my_dict = json.loads(i)
		# print('=============', json.loads(i))

	for i in my_dict.keys():
		four_parms.append(my_dict[i])
		# print('--==', four_parms)

	total_range = model(four_parms)

	print('----------', total_range)

	return total_range
	# return render_template('index.html', range=distance, four_parms=arguments)


@app.route('/')
def home():
	# a = {'a':arguments[0], 'b': arguments[1], 'c': arguments[2], 'd': arguments[3]}
	# return str(distance), str(arguments)	
	if len(distances) != 0:
		return render_template('index.html', range=distances[0], four_parms=arguments[0])
	else:
		return render_template('index.html', error='Refresh after few minute')



new_soc_val = []
@app.route('/soc', methods=['GET', "POST"])
def calculate_soc():
	global new_soc_val
	new_soc_val.clear()

	a = request.args.to_dict()

	for i in a:
		my_dict = json.loads(i)
		# print('=============', json.loads(i))

	for i in my_dict.keys():
		new_soc_val.append(my_dict[i])
	
	print('===============', new_soc_val)

	return {'new_soc': new_soc_val[-1]}


@app.route('/soc_val')
def dsiaply_soc():
	return render_template('soc.html', soc_parms=new_soc_val)



if __name__ == '__main__':
    app.run()