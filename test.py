import schedule
import time
import pandas as pd
import numpy as np
from preprocess import *


from flask import Flask
from flask_apscheduler import APScheduler

app = Flask(__name__)

class Config(object):
    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()


# new_data = pd.read_csv('data/new_data_okla.csv')


# interval examples
# @scheduler.task('interval', id='do_job_1', seconds=3, misfire_grace_time=900)
# def job1():
#     print('Job 1 executed')


# # cron examples
# @scheduler.task('cron', id='do_job_2', minute='*')
# def job2():
#     print('Job 2 executed')


# @scheduler.task('cron', id='do_job_3', week='*', day_of_week='mon')
# def job3():
#     print('Job 3 executed')


# @scheduler.task('interval', id='do_job_1', seconds=40, misfire_grace_time=900)
# @app.route('/')
# def home():
# 	data = test()
# 	print(data)
# 	return str(data)



# call the data every 30 second
# @scheduler.task('interval', id='do_job_1', seconds=20, misfire_grace_time=900)
# def data():
# 	global range_arguments
# 	range_arguments = filter_data()
# 	print('--- data',range_arguments)
# 	return None


# @scheduler.task('interval', id='do_job_2', seconds=30, misfire_grace_time=900)
# @app.route('/')
# def home():
# 	aima_catBoost_model = joblib.load(os.path.join(os.getcwd(), 'models/okla_catboost.sav'))

# 	ride_range = aima_catBoost_model.predict(np.array(range_arguments))

# 	print('--- -------------------------------- Range -----------------------')
# 	print(ride_range)
# 	print('--- -------------------------------- -----------------------',ride_range)

# 	distance = {'arguments': [list(i) for i in range_arguments.values],'range': round(ride_range.tolist()[0],2)}

# 	return distance