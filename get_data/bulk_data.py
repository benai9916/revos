from elasticsearch import Elasticsearch, helpers, exceptions

import json
import pandas
import time

# declare an instance of the Elasticsearch library
client = Elasticsearch("https://search-rev-elastic-pavbwqniud3xfj2ufsvvrkiive.ap-south-1.es.amazonaws.com", timeout=30)

# set client to 'None' if invalid
try:
    # get information on client
    client_info = Elasticsearch.info(client)

    print ('Elasticsearch client info:', json.dumps(client_info, indent=4))
except exceptions.ConnectionError as err:
    print ('Elasticsearch client error:', err)
    client = None

if client != None:

    search_body_v = {
        "query": {
            "bool": {
                "must": [
                    {"match": {'vin': 'OKLA_OFFICE_WHITE'}},
                    {"match": {'type': 1}},
                ]
            }
        }
    }

    # # make a search() request to scroll documents
    # resp = client.search(
    #     index = "events-rev-es-vehicle000",
    #     size= 8900,
    #     body = search_body_v,
    #     scroll = '10s', # time value for search
    #     _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
    #             'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit', 'gps_speed'],
    #     request_timeout=30

    # )
    # print ("total docs:", len(resp["hits"]["hits"]))

    # # get the JSON response's scroll_id
    # scroll_id = resp['_scroll_id']

    # # scroll Elasticsearch docs with scroll() method
    # resp = client.scroll(
    #     scroll_id = scroll_id,
    #     scroll = '10s', # time value for search
    #     request_timeout=30
    # )

    # print ('scroll() query length:', len(resp))

    # # get the JSON response's scroll_id
    # scroll_id = resp['_scroll_id']

    # call the helpers library's scan() method to scroll
    resp = helpers.scan(
        client,
        scroll = '10m',
        size = 10,
        _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
        'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit', 'gps_speed'],
        query=search_body_v,
        request_timeout=30
    )

    #  create an empty Pandas DataFrame object for docs
    docs = pandas.DataFrame()

    # enumerate the documents
    try:
        for num, doc in enumerate(resp):
            if num != 300000:
                print ('\n----------------------------------', num, doc['_source'])
                print(doc['_id'], doc['_source'])

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
        docs.to_csv("data/bulk_data.csv", ",")
        
    # export Elasticsearch documents to a CSV file
    docs.to_csv("data/bulk_data.csv", ",")


# start = time.time()
# res = client.search(
#     index = 'events-rev-es-vehicle000',
#     scroll = '2m',
#     size = 10,
#     body = search_body_v,
#     _source= ['vin','tripId','type' ,'timestamp','batteryCurrent', 'batteryVoltage', 'latitude', 
#         'longitude', 'throttle', 'wheelRpm', 'underVoltageLimit', 'overVoltageLimit', 'gps_speed'])

# counter = 0
# sid = res['_scroll_id']
# scroll_size = len(res['hits']['hits'])
# # scroll_size = scroll_size['value']

# print(scroll_size, sid)

# while(scroll_size > 0):

#     page = client.scroll(
#         scroll_id=sid,
#         scroll= '10m')

#     sid = page['_scroll_id']

#     scroll_size = len(page['hits']['hits'])

#     docs = pandas.DataFrame()

    # enumerate the documents
    # try:
    #     for num, doc in enumerate(page['hits']['hits']):
    #         if num != 300000:
    #             print ('\n----------------------------------', num, doc['_source'])
    #             print(doc['_id'], doc['_source'])

    #             # get _source data dict from document
    #             source_data = doc["_source"]

    #             # get _id from document
    #             _id = doc["_id"]

    #             # create a Series object from doc dict object
    #             doc_data = pandas.Series(source_data, name = _id)

    #             # append the Series object to the DataFrame object
    #             docs = docs.append(doc_data)
    #         else:
    #             break
    # except Exception as e:
    #     print('Error', e)
    #     docs.to_csv("new.csv", ",")
        
    # # export Elasticsearch documents to a CSV file
    # docs.to_csv("new.csv", ",")

#     counter = counter + 1

# print("Total page", counter)

# end = time.time() - start

# print('Total time', end)