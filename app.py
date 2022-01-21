
import json
import sys
import logging
import requests


from pathlib import Path

import psycopg2 as pg
import pandas.io.sql as psql


from flask import Response
from flask import Flask
from flask import render_template
from flask import session
from flask import request
from flask import abort, redirect, url_for
from flask import current_app, g

from esco_utils import * 

def read_template(name):
    occ_template = []
    with open(f'templates/{name}.html','r') as f:
        occ_template = ''.join([l for l in f.readlines()])
    return occ_template

def load_skillLab_DB():
    DB_USER = "amir"
    DB_PASS = '8JF9hb!7VD@L'

    VAC_PORT = "5432"
    VAC_DBNAME = "postgres"
    VAC_HOST = "vacancies"

    SKILL_PORT = "5432"
    SKILL_DBNAME = "web_production"
    SKILL_HOST = "skillmap"

    vac_conn = pg.connect(user=DB_USER,
                    password=DB_PASS,
                    host=VAC_HOST,
                    port=VAC_PORT,
                    database=VAC_DBNAME)

    skill_conn = pg.connect(user=DB_USER,
                    password=DB_PASS,
                    host=SKILL_HOST,
                    port=SKILL_PORT,
                    database=SKILL_DBNAME)
    return vac_conn, skill_conn


def sql_all_tags(lang,conn):
    occupation_local = psql.read_sql(f"""
        SELECT * FROM
        (SELECT * FROM occupations WHERE data_set='esco') AS occ 
        LEFT JOIN (SELECT * FROM occupation_translations WHERE locale='{lang}') as occtr
        ON occ.id=occtr.occupation_id
        """, conn)
    return occupation_local


def esco_solr_search(text, lang, limit):
    model = "https://taxonomy-service-tugjhopyrq-ez.a.run.app"
    url = f"{model}/search"

    payload={}
    headers = {
      'Accept': 'application/json'
    }
    params = {
        'text': text,
        'language': lang,
        'limit': limit,
        'full': 'false',
    }

    response = requests.request("GET", url, headers=headers, params=params, data=payload)
    try:
        response = response.json()
    except JSONDecodeError:
        response = {}
    return response


def esco_solr_occupations(text, lang, limit, conn):
    response = esco_solr_search(text, lang, limit)
    uris = [occ['uri'] for occ in response['_embedded']['results']]
    if len(uris)==0:
        return None
    uris = str(uris).replace(']',')').replace('[','(')
    occupations = psql.read_sql(f"""
                SELECT * FROM 
                        (SELECT * FROM occupations 
                        WHERE data_set='esco'
                        AND external_id IN {uris}) AS occ 
                    LEFT JOIN 
                        (SELECT * FROM occupation_translations 
                        WHERE locale='{lang}') AS occtr
                    ON occ.id=occtr.occupation_id
                """, conn)
    return occupations
    
    

def create_app(test_config=None):
    loaded_models = dict()
    vac_conn, skill_conn = load_skillLab_DB()
    
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)
    
    # model name to function map 
    modelname2func = {'tfidf_knn': train_tfidf_knn}
    Path("models/").mkdir(parents=True, exist_ok=True)
    with open("models/log.jl","a") as f:
        f.write("");

    
    @app.route('/js/<name>', methods=['GET', 'POST'])
    def templates(name):
        with open(f'js/{name}','r') as f:
            return f.read()

    @app.route('/review',methods=['GET'])
    def view():
        if 'country' not in request.args.keys():
            country = 'GB'
        else:
            country = request.args.get('country')
        job = psql.read_sql(f"""
        SELECT * FROM jobs 
        WHERE location_country='{country}'
        LIMIT 1000
        """, vac_conn).sample().iloc[0]
        template_task = read_template('task')
        
        return template_task
    
    
    @app.route('/sample-vacancy',methods=['GET'])
    def sample_vacancy():
        country = request.args.get('country')
        job = psql.read_sql(f"""
        SELECT * FROM jobs 
        WHERE location_country='{country}'
        LIMIT 1000
        """, vac_conn).sample().iloc[0]
        return Response(job.to_json(), mimetype='application/json') 
    
    
    @app.route('/all-tags', methods=['GET'])
    def get_all_tags():
        lang = request.args.get('lang')
        occupation_local = sql_all_tags(lang, skill_conn)
        return Response(occupation_local.to_json(orient="table"), mimetype='application/json') 
    
        
    @app.route('/get-occupation', methods=['GET'])
    def get_occupation():
        id = request.args.get('id')
        lang = request.args.get('lang')
        app.logger.info(f"get-occupation, id={id}, lang={lang}")
        occ = psql.read_sql(f"""
        SELECT * FROM occupation_translations 
        WHERE locale='{lang}' 
        AND occupation_id={id}
        """, skill_conn).iloc[0]
        return Response(occ.to_json(), mimetype='application/json') 
        
        
    @app.route('/all-models', methods=['GET'])
    def get_all_models():
        # try:
        app.logger.info(f"all-models")
        df = pd.read_json('models/log.jl',lines=True)
        return Response(df.set_index('id').to_json(orient="table"), mimetype='application/json')
        # except Exception as err:
        #     return f"Error {type(err)}\nMessage: {err}"
    
    
    @app.route('/train', methods=['POST'])
    def train_model():
        # try:
        id = str(uuid.uuid1())
        response = dict(id=id)
        data = request.get_json()
        app.logger.info(f"train {data}")
        lang = data['lang']
        train_func = modelname2func[data['model_name']]
        occ_local = sql_all_tags(lang, skill_conn)

        model, params = train_func(occ_local=occ_local,**data)
        response.update(params)
        with open(f'models/{id}.pk','wb') as f:
            pickle.dump(model, f)
        with open('models/log.jl', 'a') as f:
            f.write('\n'+json.dumps(response))   
        return Response(json.dumps(response), mimetype='application/json')
        # except KeyError as err:
        #     return f"valid 'model_name' not provided: {err}" 
        # except OSError as err:
        #     return f"OS error, could not open files for model dump: {err}"
        # except Exception as err:
        #     return f"Error {type(err)}\nMessage: {err}"
        
    
    
    @app.route('/top-tags', methods=['POST'])
    def predict():
        # try:
        data = request.json
        # app.logger.info(f'top-tags: {data}')
        text, id = data['description'], data['id']
        excluded =  data['excluded'] if 'excluded' in data else []
        limit = data['limit'] if 'limit' in data else 50
        title = data['title'] if 'title' in data else ''
        noise = data['noise'] if 'noise' in data else 0.3
        if not isinstance(title,str):
            title = ''
        if not isinstance(text,str):
            text = ''
        if id in loaded_models:
            model = loaded_models[id]
        else:
            model = pickle.load(open(f'models/{id}.pk','rb'))
            loaded_models[id] = model
        
        if title==text: # apply search 
            occupations = esco_solr_occupations(text, model['meta']['lang'],limit+len(excluded), conn=skill_conn)
            response = [{'index': str(id), 'distance': 1-1.0/(i+1)} 
                        for i,id in enumerate(occupations.occupation_id.values) 
                        if id not in excluded] 
        else:
            # solr_response = []
            title = (title + ' ')*model['meta']['title_imp']
            distances, indices = predict_top_tags(model, title + ' ' + text, p=noise)
            response = [{'index': i, 'distance': d} for i,d in zip(indices,distances) if i not in excluded] 
            # response = solr_response + response
        return Response(json.dumps(response[:limit]), mimetype='application/json')
        # except KeyError as err:
        #     return f"required field not provided: {err}" 
        # except FileNotFoundError as err:
        #     return f"model id not found" 
        # except Exception as err:
        #     return f"Error {type(err)}\nMessage: {err}"
    
    @app.route('/search', methods=['GET'])
    def search_text():
        data = request.json
        app.logger.info(f'search: {data}')
        text = request.args.get('text')
        lang = request.args.get('lang')
        limit = request.args.get('limit')
        
        occupations = esco_solr_occupations(text, lang, limit, conn=skill_conn)
        return Response(occupations.to_json(orient='table'), mimetype='application/json')

    return app
