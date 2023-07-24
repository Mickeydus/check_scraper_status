# %%
import requests
import os
import dotenv
import logging
from sqlalchemy import create_engine
import azure.functions as func
from config import SCRAPER_API_URL

requests.packages.urllib3.disable_warnings()  # type: ignore

logger = logging.getLogger("logger_name")
logger.disabled = True

dotenv.load_dotenv('.env')

env_variables = ['CONNECTION_STRING','PASSWORD_DB']
for env_var in env_variables:
    logging.info(env_var)
    env_value = os.environ.get(env_var)
    if not env_value:
        logging.error(f"{env_var} is not set.")


def check_req_for_var(req: func.HttpRequest, var:str) -> str:
    variable = req.params.get(var)

    if not variable:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            variable = req_body.get(var)

    return variable

def scraper_api(request_type:str, request_id:str) -> dict:
    response = requests.get(SCRAPER_API_URL + request_type, params={'request_id': request_id}, verify=False)
    response.raise_for_status()
    return response.json()

def connect_to_db():
    uname = 'admin-mimir'
    pword = os.environ.get('PASSWORD_DB')
    server = 'innovationdatalab.database.windows.net'
    port = 1433
    dbname = 'Mimir'
    conn_string = f"mssql+pyodbc://{uname}:{pword}@{server}:{port}/{dbname}?driver=ODBC+Driver+17+for+SQL+Server"
    con = create_engine(conn_string)
    return con


db_conn = connect_to_db()
