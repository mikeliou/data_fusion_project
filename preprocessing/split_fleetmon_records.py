import pandas as pd
import numpy as np
import pymongo
from datetime import datetime

mongoClient = pymongo.MongoClient('localhost:27017')
db = mongoClient.localdb
collection = db.data_fusion

dataframe = pd.DataFrame(list(collection.find({})))

new_list = []
for index, row in dataframe.iterrows():
    if (row["source_id"] == 3):
        dictRowFrom = dict(row)
        dictRowFrom["datetime"] = row["datetime_from"]
        dictRowFrom["event"] = "Arrival"

        dictRowTo = dict(row)
        dictRowTo["datetime"] = row["datetime_to"]
        dictRowTo["event"] = "Departure"

        if (dictRowFrom["datetime"]):
            new_list.append(dictRowFrom)
        if (dictRowTo["datetime"]):
            new_list.append(dictRowTo)
    else:
        new_list.append(row)

new_df = pd.DataFrame(new_list)
del new_df['datetime_from']
del new_df['datetime_to']
new_df = new_df[new_df["datetime"].str.contains('2019-03-20')]

new_df.to_csv("data_fusion_alt.csv")
