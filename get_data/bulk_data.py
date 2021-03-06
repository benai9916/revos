from elasticsearch import Elasticsearch, helpers, exceptions

import json
import pandas
import time
import sys
import time

sys.path.insert(1, './')

from config import ES_INDEX, ES_URL


start_time = time.time()

# declare an instance of the Elasticsearch library
client = Elasticsearch(ES_URL, timeout=60)

# set client to 'None' if invalid
try:
    # get information on client
    client_info = Elasticsearch.info(client)

    print ('Elasticsearch client info:', json.dumps(client_info, indent=4))
except exceptions.ConnectionError as err:
    print ('Elasticsearch client error:', err)
    client = None
#
if client != None:

    search_body_v = {
        "query": {
            "bool": {
                "must": [
                    {"match": {'vin': 'M22YCESD20J002042'}}
                    # {"match": {"batteryMaxVoltage": 54}}
                ]
            }
        }
    }

#     # make a search() request to scroll documents
#     resp = client.search(
#         index = ES_INDEX,
#         size= 10000,
#         body = search_body_v,
#         scroll = '10m', # time value for search
#         # _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
#                 # 'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit', 'gps_speed'],
#         _source= [
#         'tripId',
#         'vin',
#         'timestamp', 
#         'relativeSOC', 
#         'batteryVoltageAdc', 
#         'gpsSpeed', 
#         'ignition', 
#         'latitude', 
#         'longitude', 
#         'ridingCurrent',
#         'chargeDischargeTimes', 
#         'totalMilage'],
#         request_timeout=30

#     )
#     # # print ("total docs:", len(resp["hits"]["hits"]))

#     # # get the JSON response's scroll_id
#     scroll_id = resp['_scroll_id']

#     # scroll Elasticsearch docs with scroll() method
#     while len(resp['hits']['hits']):
#         resp = client.scroll(
#             scroll_id = scroll_id,
#             scroll = '10m', # time value for search
#             request_timeout=30
#         )

#         # print ('scroll() query length:', len(resp))

#         # get the JSON response's scroll_id
#         # scroll_id = resp['_scroll_id']

        # call the helpers library's scan() method to scroll
    resp = helpers.scan(
        client,
        scroll = '10m',
        size = 10000,
        query=search_body_v,
        request_timeout=30
    )

#         #  create an empty Pandas DataFrame object for docs
    docs = pandas.DataFrame()

#         # print(resp['hits']['hits'])


        # enumerate the documents
    try:
        for num, doc in enumerate(resp):
            if num != 3:
                print ('\n----------------------------------', num, doc['_source'])
                # print(doc['_id'], doc['_source'])

                # get _source data dict from document
                source_data = doc["_source"]

                # get _id from document
                _id = doc["_id"]

                # create a Series object from doc dict object
                doc_data = pandas.Series(source_data, name = _id)

                # append the Series object to the DataFrame object
                docs = docs.append(doc_data)
            else:
                break
    except Exception as e:
        print('Error', e)
        docs.to_csv("get_data/datas/M22YCESD20J002042.csv", ",")
        
    # export Elasticsearch documents to a CSV file
    docs.to_csv("get_data/datas/M22YCESD20J002042.csv", ",")

print('Total time: ', time.time() - start_time)


# start = time.time()
# res = client.search(
#     index = ES_INDEX,
#     scroll = '10m',
#     size = 10000,
#     body = search_body_v,
#     _source= [
#     'tripId',
#     'vin',
#     'timestamp', 
#     'relativeSOC', 
#     'batteryVoltageAdc', 
#     'gpsSpeed', 
#     'ignition', 
#     'latitude', 
#     'longitude', 
#     'ridingCurrent',
#     'chargeDischargeTimes', 
#     'totalMilage'])

# counter = 0
# sid = res['_scroll_id']
# scroll_size = len(res['hits']['hits'])
# # scroll_size = scroll_size['value']

# print(scroll_size, sid)

# print('=============================', counter)

# while(scroll_size > 0):

#     page = client.scroll(
#         scroll_id=sid,
#         scroll= '10m')

#     sid = page['_scroll_id']

#     scroll_size = len(page['hits']['hits'])

#     docs = pandas.DataFrame()

#     # enumerate the documents
#     try:
#         for num, doc in enumerate(page['hits']['hits']):
#             if num != 300000:
#                 print (num, doc['_source'])
#                 print(doc['_id'], doc['_source'])

#                 # get _source data dict from document
#                 source_data = doc["_source"]

#                 # get _id from document
#                 _id = doc["_id"]

#                 # create a Series object from doc dict object
#                 doc_data = pandas.Series(source_data, name = _id)

#                 # append the Series object to the DataFrame object
#                 docs = docs.append(doc_data)
#             else:
#                 break
#     except Exception as e:
#         print('Error', e)
#         docs.to_csv("get_data/datas/M22YCESD20J002042-42.csv", ",")
        
#     # export Elasticsearch documents to a CSV file
#     docs.to_csv("get_data/datas/M22YCESD20J002042.csv-42", ",")

#     counter = counter + 1

# print("Total page", counter)

# end = time.time() - start

# print('Total time', end)