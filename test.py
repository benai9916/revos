import schedule
import time
import pandas as pd
import numpy as np
from preprocess import *

# new_data = pd.read_csv('data/new_data_okla.csv')

# def job():
# 	# ids = id
# 	return print('idsss')
# 	# return print(new_data['tripId'].head(1))
#     # print('executed')



# for id in new_data.tripId.value_counts().index.to_list()[0:1]: 
#   # min battery Voltage
#   min_battery_voltage = new_data['underVoltageLimit'].unique().min()

#   # inject each trip_id
#   trip = new_data.loc[new_data['tripId'] == id].sort_values(by='time', ascending=True)

#   # call the function
#   final_df1 = schedule.every(10).seconds.do(job)


# # print(ids)

# while True:
#     schedule.run_pending()
#     time.sleep(1)


# from flask import Flask
# from flask_apscheduler import APScheduler

# import time
# import atexit

# from apscheduler.schedulers.background import BackgroundScheduler



 
# app = Flask(__name__)

 
# def scheduled_task():
#     print('Task  running iteration')
 
# @app.route('/')
# def welcome():
#     return 'Welcome to flask_apscheduler demo', 200
 

# scheduler = BackgroundScheduler()
# scheduler.add_job(func=scheduled_task, trigger="interval", seconds=3)
# scheduler.start()


# atexit.register(lambda: scheduler.shutdown())

# if __name__ == '__main__':
# 	app.run()


from flask import Flask
from flask_apscheduler import APScheduler

app = Flask(__name__)

class Config(object):
    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()


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



# if __name__ == '__main__':
#     # app = Flask(__name__)
#     app.config.from_object(Config())

#     # it is also possible to enable the API directlstatus_codey
#     # scheduler.api_enabled = True
#     scheduler.init_app(app)
#     scheduler.start()

#     app.run()

import requests
import schedule
import time

def run_run():
	res = requests.get('http://127.0.0.1:5000/range?tripDuration=0&GpsSpeed=0&voltageDrop=0&distance=0&availableAdc=70')
	print(res.json())


schedule.every(5).seconds.do(run_run)

while True:
    schedule.run_pending()
    time.sleep(1)