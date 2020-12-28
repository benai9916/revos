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


def model(parms):
	global  arguments

	okla_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/cb_combine.sav'))

	parms = [i for i in json.loads((parms[0]))[0]]

	arguments = parms

	ranges = okla_catBoost_model.predict(np.array([parms]))

	distance = {'range': round(ranges.tolist()[0],2)}

	return distance


@app.route('/')
def home():
	try:
	# a = {'a':arguments[0], 'b': arguments[1], 'c': arguments[2], 'd': arguments[3]}
		return render_template('index.html', range=distance, four_parms=arguments)
	except:
		return 'please reload'


@app.route('/range', methods=['GET'])
def calculate_range():
	global distance
	global four_parms
	four_parms = request.args.getlist('data')

	distance = model(four_parms)
	
	return distance



if __name__ == '__main__':
    app.run()