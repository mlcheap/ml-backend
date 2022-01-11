
import json
import sys

from pathlib import Path

import psycopg2 as pg

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
    if lang=='ar':
        occupation_local = psql.read_sql(f"""
        SELECT * FROM occupation_translations
        WHERE description IS NOT NULL
        AND locale='{lang}'
        """, conn)
    else:
        occupation_local = psql.read_sql(f"""
        SELECT * FROM occupation_translations
        WHERE alternates IS NOT NULL
        AND locale='{lang}'
        """, conn)
    return occupation_local
    

def create_app(test_config=None):
    vac_conn, skill_conn = load_skillLab_DB()
    
    app = Flask(__name__)
    
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
        occ = psql.read_sql(f"""
        SELECT * FROM occupation_translations 
        AND locale='{lang}' 
        AND occupation_id={id}
        """, skill_conn)
        return Response(occ.to_json(), mimetype='application/json') 
        
        
    @app.route('/all-models', methods=['GET'])
    def get_all_models():
        try:
            df = pd.read_json('models/log.jl',lines=True)
            return Response(df.set_index('id').to_json(orient="table"), mimetype='application/json')
        except Exception as err:
            return f"Error {type(err)}\nMessage: {err}"
    
    
    @app.route('/train', methods=['POST'])
    def train_model():
        try:
            id = str(uuid.uuid1())
            response = dict(id=id)
            data = request.get_json()
            train_func = modelname2func[data['model_name']]
            lang = data['lang']
            occ_local = sql_all_tags(lang, skill_conn)
            #occ_local = psql.read_sql(f"""
            #SELECT * FROM occupation_translations 
            #WHERE alternates IS NOT NULL 
            #AND locale='{lang}'
            #""", skill_conn)

            model, params = train_func(occ_local=occ_local,**data)
            response.update(params)
            with open(f'models/{id}.pk','wb') as f:
                pickle.dump(model, f)
            with open('models/log.jl', 'a') as f:
                f.write('\n'+json.dumps(response))   
            return Response(json.dumps(response), mimetype='application/json')
        except KeyError as err:
            return f"valid 'model_name' not provided: {err}" 
        except OSError as err:
            return f"OS error, could not open files for model dump: {err}"
        except Exception as err:
            return f"Error {type(err)}\nMessage: {err}"
        
    
    
    @app.route('/top-tags', methods=['POST'])
    def predict():
        try:
            data = request.json
            text, id = data['description'], data['id']
            title = data['title'] if ('title' in data.keys()) else ''
            model = pickle.load(open(f'models/{id}.pk','rb'))
            title = (title + ' ')*model['meta']['title_imp']
            distances, indices = predict_top_tags(model, title + '\n' + text)
            response = [{'index': i, 'distance': d} for i,d in zip(indices,distances)] 
            return Response(json.dumps(response), mimetype='application/json')
        except KeyError as err:
            return f"required field not provided: {err}" 
        except FileNotFoundError as err:
            return f"model id not found" 
        except Exception as err:
            return f"Error {type(err)}\nMessage: {err}"

    return app
