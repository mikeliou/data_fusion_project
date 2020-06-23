from __future__ import print_function
from future.builtins import next

import os
import csv
import re
import collections
import logging
import optparse
import numpy
from pymongo import MongoClient
import pymongo
import json
import dedupe
import pprint
from unidecode import unidecode
import datetime
from dateutil import parser
import pandas

dt_mins_window = 30
mongoClient = MongoClient('localhost:27017')
db = mongoClient.localdb
collection = db.data_fusion

df_source1 = pandas.DataFrame(list(collection.find({"source_id": 1})))
list_merge_source_1_2 = []
for indexSource1, rowSource1 in df_source1.iterrows():
    dateVal = parser.parse(rowSource1["datetime"])
    dateStart = dateVal - datetime.timedelta(minutes=dt_mins_window)
    dateEnd = dateVal + datetime.timedelta(minutes=dt_mins_window)

    df_source2 = pandas.DataFrame(list(collection.find({"$and": [
                                                                {"source_id": 2},
                                                                {"vessel_name": rowSource1["vessel_name"]},
                                                                {"event": rowSource1["event"]},
                                                                {"datetime": {'$gte': str(dateStart), '$lte': str(dateEnd)}}
                                                            ]
                                                            })))

    for indexSource2, rowSource2 in df_source2.iterrows():
        dateSource1 = parser.parse(rowSource1["datetime"])
        dateSource2 = parser.parse(rowSource2["datetime"])
        elapsedTime = dateSource1 - dateSource2
        meanDateVal = dateSource1 - datetime.timedelta(seconds=(elapsedTime.total_seconds() / 2.0))

        list_merge_source_1_2.append([meanDateVal, rowSource2["vessel_name"], rowSource2["event"]])

df_merge_source_1_2 = pandas.DataFrame(list_merge_source_1_2)
df_merge_source_1_2.columns = ['datetime', 'vessel_name', 'event']
df_merge_source_1_2.to_csv('df_merge_source_1_2.csv', encoding='utf-8')

#df_merge_source_1_2 = pandas.read_csv('clean_gold_standard_1_2.csv')
list_merge_source_total = []
for indexMergeSource12, rowMergeSource12 in df_merge_source_1_2.iterrows():
    if (rowMergeSource12.notnull().any()):
        if (rowMergeSource12["event"] == "Arrival"):
            dateField = "datetime_from"
        else:
            dateField = "datetime_to"

        dateVal = rowMergeSource12["datetime"]
        dateStart = dateVal - datetime.timedelta(minutes=dt_mins_window)
        dateEnd = dateVal + datetime.timedelta(minutes=dt_mins_window)

        df_source3 = pandas.DataFrame(list(collection.find({"$and":[
                                                                    {"source_id": 3},
                                                                    {"vessel_name": rowMergeSource12["vessel_name"]},
                                                                    {dateField: {'$gt': str(dateStart), '$lt': str(dateEnd)}}
                                                                ]
                                                         })))
        for indexSource3, rowSource3 in df_source3.iterrows():
            dateSourceMerge12 = rowMergeSource12["datetime"]
            dateSource3 = parser.parse(rowSource3[dateField])
            elapsedTime = dateSourceMerge12 - dateSource3
            meanDateVal = dateSourceMerge12 - datetime.timedelta(seconds=(elapsedTime.total_seconds() / 2.0))

            list_merge_source_total.append([meanDateVal, rowMergeSource12["vessel_name"], rowMergeSource12["event"]])


df_merge_source_total = pandas.DataFrame(list_merge_source_total)
df_merge_source_total.columns = ['datetime', 'vessel_name', 'event']
df_merge_source_total.to_csv('df_merge_source_total.csv', encoding='utf-8')
df_merge_source_total.to_json('df_merge_source_total.json')
