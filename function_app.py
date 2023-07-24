import os
import azure.functions as func
from utils.scrapertoolkit import connect_to_db, scraper_api, db_conn
import pandas as pd
import requests
import logging
import json

verify_ssl = False

def main(mytimer: func.TimerRequest) -> None:
    # Connect to the database
    db_conn = connect_to_db()

    # Define the query to select rows where ScraperStatus == 'In Progress'
    select_query = "SELECT RequestID FROM ChangeTrackingTable WHERE ScraperStatus = 'In Progress'"

    # Execute the query
    cursor = db_conn.cursor()
    cursor.execute(select_query)

    # For each row where ScraperStatus == 'In Progress'
    for row in cursor:
        request_id = row[0]

        # Call GetScraperStatus_function
        scraper_status = GetScraperStatus_function(request_id)
        
        # If scraper_status == 'Completed', update ScraperStatus in both tables
        if scraper_status == 'RUNSTATUS.Completed':
            update_query_scraper_results = f"UPDATE scraper_results SET ScraperStatus = 'Completed' WHERE RequestID = '{request_id}'"
            update_query_change_tracking = f"UPDATE ChangeTrackingTable SET ScraperStatus = 'Completed' WHERE RequestID = '{request_id}'"

            cursor.execute(update_query_scraper_results)
            cursor.execute(update_query_change_tracking)
            GetScraperResults_function(request_id)

    db_conn.commit()
    trigger_pipeline('org_class_pipeline')

def GetScraperStatus_function(request_id: str) -> str:
    scraper_status = scraper_api(request_type='status' ,request_id=request_id)
    # nested dictionary for config that is loaded as a string by default, so little transform
    scraper_status['status'] = json.loads(scraper_status['status'])

    return scraper_status


def GetScraperResults_function(request_id: str) -> str:
    scrape_results = scraper_api(request_type='results', request_id=request_id)
    scraper_status = scraper_api(request_type='status', request_id=request_id)
    website_url = json.loads(scraper_status['config'])['url']

    for page in scrape_results:
        page['text'] = requests.get(page['blob_url'], verify=verify_ssl).text
        # This will simply ignore characters that can't be printed
        page['text'] = ''.join(i for i in page['text'] if ord(i) < 128)

        link_text = {
            "scraper_id": [request_id],
            "page_id": [page['id']],
            "website_url": [website_url],
            "page_url": [page['url']],
            "text": [page['text']],
           
        }
        #  "case_version_id": [case_version_id],g
        link_text_df = pd.DataFrame(link_text)
        with db_conn.connect() as conn:
            link_text_df.to_sql("link_text", conn, if_exists='append', index=False)

    return scrape_results


def trigger_pipeline(pipeline_name: str):
    logging.info('Python function processed a request.')
    # Tenant ID, Client ID, and Client Secret can be saved in Azure Function App Settings
    tenant_id = "bf8ba62d-740b-46e9-a7da-cfe794c7da80"
    client_id = "e0cc531c-8c0b-4af8-b083-8f6da7c3530d"
    client_secret = "AuU8Q~OYO4CaobaO0reWpNmzPkrRHpCPIcuF3cyO"

    # Name of your Azure Subscription, Resource Group, and Data Factory
    subscription_id = "10d893ed-2b9c-4013-90ad-4210e35311a3"
    resource_group_name = "rg-mimir"
    data_factory_name = "InnovatieDataFactory"

    # URL we will post to in order to get the access token
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    # Data that will be passed in the post request
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': "https://management.azure.com/"
    }

    # Headers for the post request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Make the post request
    response = requests.post(url, data=payload, headers=headers)

    # Extract the access token from the response
    access_token = response.json()['access_token']

    # REST API endpoint to create a pipeline run
    pipeline_run_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{data_factory_name}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"

    # Create a JSON object containing the pipeline parameters
    pipeline_parameters = {
    'parameters': {
        'case_version_id': 'case_version_id'
    }
}

    # Headers for the pipeline run post request
    pipeline_run_headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {access_token}"
    }

    # Create a pipeline run
    pipeline_run_response = requests.post(pipeline_run_url, headers=pipeline_run_headers, json=pipeline_parameters)

    if pipeline_run_response.status_code == 200:
        logging.info(f'Successfully started the pipeline: {pipeline_name}')
        return "Successfully started the pipeline."
    else:
        logging.info('jemoeder')
        logging.error(f'Failed to start the pipeline: {pipeline_name}. Response: {pipeline_run_response.content}')
        return f"Failed to start the pipeline. Response: {pipeline_run_response.content}"
