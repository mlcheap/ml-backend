import json
import os
import re
import glob
import uuid
import pickle
import tqdm 
import datetime

import pandas as pd
import numpy as np
import psycopg2 as pg
import pandas.io.sql as psql
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer


def occ_alt_stringify(alt,title_imp=1):
    alternates = ""
    if isinstance(alt,str):
        alternates = alt[1:-2].replace("'","") * title_imp if alt else ""
    elif isinstance(alt,list):
        alternates = ', '.join(alt*title_imp) if alt else ' '
    return alternates

        
def occ_stringify(occ, title_imp=1,alt_title_imp=1,case_insensitive=False):
    title = ' '.join([occ.title]*title_imp)
    alternates = occ_alt_stringify(occ.alternates,alt_title_imp)
    desc = '\n'.join([title, alternates, occ.description])
    if case_insensitive:
        desc = desc.lower()
    return desc


def job_stringify(job,title_imp=1, case_insensitive=False):
    title = ' '.join([job.job_title]*title_imp)
    desc = '\n'.join([title,job.job_description])
    if case_insensitive:
        desc = desc.lower()
    return desc


def train_tfidf_knn(occ_local, model_name,lang,ngram_min=1,ngram_max=4,n_neighbors = 5, title_imp=5,alt_title_imp=5,case_insensitive=True):
    assert(model_name=='tfidf_knn')
    strings = [occ_stringify(occ,title_imp,alt_title_imp,case_insensitive) for occ in occ_local.itertuples()]
    vectorizer = TfidfVectorizer(ngram_range=(ngram_min,ngram_max))
    X = vectorizer.fit_transform(strings)
    knn_index = NearestNeighbors(n_neighbors=n_neighbors).fit(X)
    feature_names = vectorizer.get_feature_names_out()
    
    meta = dict(model_name=model_name,
                lang=lang,
                ngram_min=ngram_min,
                ngram_max=ngram_max,
                n_neighbors=n_neighbors,
                title_imp=title_imp,
                alt_title_imp=alt_title_imp,
                case_insensitive=case_insensitive)
    
    model = dict(meta=meta,
                 occupation_id=occ_local.occupation_id.tolist(), 
                 vectorizer=vectorizer, 
                 feature_names=feature_names, 
                 knn_index=knn_index)
    return model, meta


def predict_top_tags(model, text):
    occ_id,vectorizer,knn_index = model['occupation_id'], model['vectorizer'], model['knn_index'] 
    X = vectorizer.transform([text])
    distances, indices = model['knn_index'].kneighbors(X)
    occ_indices = [int(occ_id[i]) for i in indices[0]]
    confidence = [float(d) for d in distances[0]]
    return confidence, occ_indices
    
    
