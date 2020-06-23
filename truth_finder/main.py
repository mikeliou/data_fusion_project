import pandas as pd
import numpy as np
from numpy.linalg import norm
from sklearn.feature_extraction.text import TfidfVectorizer
from truthdiscovery import TruthFinder
import pymongo
from datetime import datetime


mongoClient = pymongo.MongoClient('localhost:27017')
db = mongoClient.localdb
collection = db.data_fusion_alt

dataframe = pd.DataFrame(list(collection.find({})))

#dataframe = dataframe[dataframe["datetime"].str.contains('2019-03-20')]
dataframe["datetime"] = dataframe["datetime"].str.split(' ').str[1]

print(dataframe.shape[0])

'''print(dataframe.head())
for index, row in dataframe.iterrows():
    dataframe.at[index, "datetime"] = datetime.timestamp(dataframe.at[index, "datetime"])
    print(dataframe.at[index, "datetime"])
print(dataframe.head())'''

vectorizer = TfidfVectorizer(min_df=1)
vectorizer.fit(dataframe["datetime"])

def similarity(w1, w2):
    V = vectorizer.transform([w1, w2])
    v1, v2 = np.asarray(V.todense())
    return np.dot(v1, v2) / (norm(v1) * norm(v2))


def implication(f1, f2):
    return similarity(f1.lower(), f2.lower())


finder = TruthFinder(implication, dampening_factor=0.3, influence_related=0.5)

print("Inital state")
print(dataframe.head())
dataframe = finder.train(dataframe)

print("Estimation result")
print(dataframe.head())

dataframe.to_csv('truth_finder_results.csv')
