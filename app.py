from flask import Flask, request, jsonify
import os
from flask_cors import CORS 
import joblib
import json
import numpy as np

# ml libraries
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_squared_log_error, r2_score, explained_variance_score

from preprocess import *

from flask_apscheduler import APScheduler

import time
import atexit

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
app.config['DEBUG'] = True

class Config(object):
    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()


# call the data every 30 second
@scheduler.task('interval', id='do_job_1', seconds=30, misfire_grace_time=900)
def data():
	global range_arguments
	range_arguments = filter_data()
	# print('--- data',range_arguments)
	return None


@app.route('/')
def home():
	aima_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/okla_catboost.sav'))

	ride_range = aima_catBoost_model.predict(np.array(range_arguments))

	print('--- -------------------------------- Range -----------------------')
	print(ride_range)
	print('--- -------------------------------- -----------------------',ride_range)

	return str(ride_range)


@app.route('/range', methods=['GET'])
def calculate_range():

	aima_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/aima_catboost.sav'))

	parms = request.args


	ranges = aima_catBoost_model.predict(np.array([[
											int(parms['tripDuration']),
											int(parms['GpsSpeed']),
											int(parms['voltageDrop']),
											int(parms['distance']),
											int(parms['availableAdc'])
										]]))

	distance = {'range(km)': round(ranges.tolist()[0],2)}

	
	return json.dumps(distance)



if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler.init_app(app)
    scheduler.start()

    app.run()
