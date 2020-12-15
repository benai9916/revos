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

# from preprocess import *

from flask_apscheduler import APScheduler

import time
# import atexit

# from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
# from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
cors = CORS(app)

app.config['DEBUG'] = True

class Config(object):
    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()


# for i in range(1):
# 	range_arguments = filter_data()

@app.route('/')
def home():
	return render_template('index.html')


@app.route('/range', methods=['GET'])
def calculate_range():

	okla_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/okla_catboost.sav'))

	parms = request.args.getlist('data')


	parms = [i for i in json.loads((parms[0]))[0]]

	ranges = okla_catBoost_model.predict(np.array([parms]))

	distance = {'range': round(ranges.tolist()[0],2)}
	
	return distance



if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler.init_app(app)
    scheduler.start()

    app.run()
