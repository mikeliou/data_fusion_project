#!/usr/bin/python
# -*- coding: utf-8 -*-
from future.builtins import next
import os
import csv
import re
import collections
import logging
import optparse
from pymongo import MongoClient
from numpy import nan
import pandas as pd
import dedupe
import pprint
from unidecode import unidecode

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose :
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)


# ## Setup

input_file = 'data_fusion_alt.csv'
output_file = 'data_fusion_output.csv'
settings_file = 'data_fusion_learned_settings'
training_file = 'data_fusion_training.json'


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    import unidecode
    column = column.decode("utf8")
    column = unidecode.unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()

    if "20/03/2019 " in column:
        column = column.replace("20/03/2019 ", "")

    if not column :
        column = None
    return column

data_d = {}
def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = row['_id']
            data_d[row_id] = dict(clean_row)

    return data_d

def readDataMongo():
    data_d = {}
    # Connection to the MongoDB Server
    mongoClient = MongoClient('localhost:27017')
    # Connection to the database
    db = mongoClient.localdb
    # Collection
    collection = db.data_fusion_alt
    dataframe = pd.DataFrame(list(collection.find({})))
    dataframe.to_csv("data_fusion_alt.csv")
    for row in collection.find({}):
        clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
        row_id = row['_id']
        data_d[row_id] = dict(clean_row)

    return data_d

print('importing data ...')
data_d = readData("data_fusion_alt.csv")

# ## Training

if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field.
    fields = [
        {'field' : 'event', 'type': 'Exact'},
        {'field' : 'vessel_name', 'type': 'Exact'},
        {'field' : 'datetime', 'type': 'String'}
        ]

    # Create a new deduper object and pass our data model to it.
    print('dsa')
    deduper = dedupe.Dedupe(fields)

    # To train dedupe, we feed it a sample of records.
    deduper.sample(data_d, 15000)


    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file, 'rb') as f:
            deduper.readTraining(f)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('starting active labeling...')

    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'wb') as sf :
        deduper.writeSettings(sf)


# ## Blocking

print('blocking...')

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall.
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = deduper.threshold(data_d, recall_weight=2)

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print('clustering...')
clustered_dupes = deduper.match(data_d, threshold)
pd.DataFrame(clustered_dupes).to_csv("clustered_dupes.csv")

print('# duplicate sets', len(clustered_dupes))

# ## Writing Results

# Write our original data back out to a CSV with a new column called
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data_d[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id, score in zip(id_set, scores) :
        cluster_membership[record_id] = {
            "cluster id" : cluster_id,
            "canonical representation" : canonical_rep,
            "confidence": score
        }

singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output:
    writer = csv.writer(f_output)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = next(reader)
        heading_row.insert(0, 'confidence_score')
        heading_row.insert(0, 'Cluster ID')

        writer.writerow(heading_row)

        list_rows = []
        for row in reader:
            row_id = row[0]
            if row_id in cluster_membership :
                cluster_id = cluster_membership[row_id]["cluster id"]
                row.insert(0, cluster_membership[row_id]['confidence'])
                row.insert(0, cluster_id)

                list_rows.append(row)

        output_df = pd.DataFrame(list_rows)
        output_df.columns = heading_row
        output_df.sort_values("Cluster ID").to_csv(output_file)
