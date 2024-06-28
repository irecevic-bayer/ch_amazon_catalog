'''

The following code loads credentials for Amazon Vendor central around the world (EMEA, NA, FE) and downloads
product catalog based on product sales (data from BigQuery). 

Code gathers ACCESS_TOKEN from Amazon services in order to be able to perform API call for Catalog listing.

Iterating through ASIN list, code picks up payload and stores it in Bayer Google Cloud storage bucket, clasified by
date and market name, creating HIVE structure.

'''

import os
import json
import time
from google.cloud import bigquery, storage
from jsonschema import validate, ValidationError
import requests
import datetime
from enum import Enum

# Define the minimum output schema
schema = {
    "type": "object",
    "properties": {
        "asin": {"type": "string"},
        "identifiers": {
        },
        "summaries": {
        },
        "vendorDetails": {
        }
    },
    "required": ["asin", "identifiers", "summaries", "vendorDetails"]
}

# mapping markets with endpoints
BASE_URL = 'https://sellingpartnerapi'

class Marketplaces(Enum):
    """Enumeration for MWS marketplaces, containing endpoints and marketplace IDs.
    Example, endpoint and ID for UK marketplace:
        endpoint = Marketplaces.UK.endpoint
        marketplace_id = Marketplaces.UK.marketplace_id
    """

    #: Amazon marketplace in United Arab Emirates (AE)
    AE = (f"{BASE_URL}-eu.amazon.com", "A2VIGQ35RCS4UG", "eu-west-1")

    #: Amazon marketplace in Belgium (BE)
    BE = (f"{BASE_URL}-eu.amazon.com", "AMEN7PMS3EDWL", "eu-west-1")

    #: Amazon marketplace in Germany (DE)
    DE = (f"{BASE_URL}-eu.amazon.com", "A1PA6795UKMFR9", "eu-west-1")

    #: Amazon marketplace in Poland (PL)
    PL = (f"{BASE_URL}-eu.amazon.com", "A1C3SOZRARQ6R3", "eu-west-1")

    #: Amazon marketplace in Egypt (EG)
    EG = (f"{BASE_URL}-eu.amazon.com", "ARBP9OOSHTCHU", "eu-west-1")

    #: Amazon marketplace in Spain (ES)
    ES = (f"{BASE_URL}-eu.amazon.com", "A1RKKUPIHCS9HS", "eu-west-1")

    #: Amazon marketplace in France (FR)
    FR = (f"{BASE_URL}-eu.amazon.com", "A13V1IB3VIYZZH", "eu-west-1")

    #: Amazon marketplace in Great Britain (GB)
    GB = (f"{BASE_URL}-eu.amazon.com", "A1F83G8C2ARO7P", "eu-west-1")

    #: Amazon marketplace in India (IN)
    IN = (f"{BASE_URL}-eu.amazon.com", "A21TJRUUN4KGV", "eu-west-1")

    #: Amazon marketplace in Italy (IT)
    IT = (f"{BASE_URL}-eu.amazon.com", "APJ6JRA9NG5V4", "eu-west-1")

    #: Amazon marketplace in Netherlands (NL)
    NL = (f"{BASE_URL}-eu.amazon.com", "A1805IZSGTT6HS", "eu-west-1")

    #: Amazon marketplace in Saudi Arabia (SA)
    SA = (f"{BASE_URL}-eu.amazon.com", "A17E79C6D8DWNP", "eu-west-1")

    #: Amazon marketplace in Sweden (SE)
    SE = (f"{BASE_URL}-eu.amazon.com", "A2NODRKZP88ZB9", "eu-west-1")

    #: Amazon marketplace in Turkey (TR)
    TR = (f"{BASE_URL}-eu.amazon.com", "A33AVAJ2PDY3EV", "eu-west-1")

    #: Amazon marketplace in United Kingdom (UK) - alias for GB
    UK = (f"{BASE_URL}-eu.amazon.com", "A1F83G8C2ARO7P", "eu-west-1")

    #: Amazon marketplace in South Africa (ZA)
    ZA = (f"{BASE_URL}-eu.amazon.com", "AE08WJ6YKNBMC", "eu-west-1")

    #: Amazon marketplace in Australia (AU)
    AU = (f"{BASE_URL}-fe.amazon.com", "A39IBJ37TRP1C6", "us-west-2")

    #: Amazon marketplace in Japan (JP)
    JP = (f"{BASE_URL}-fe.amazon.com", "A1VC38T7YXB528", "us-west-2")

    #: Amazon marketplace in Singapore (SG)
    SG = (f"{BASE_URL}-fe.amazon.com", "A19VAU5U5O7RUS", "us-west-2")

    #: Amazon marketplace in United States (US)
    US = (f"{BASE_URL}-na.amazon.com", "ATVPDKIKX0DER", "us-east-1")

    #: Amazon marketplace in Brazil (BR)
    BR = (f"{BASE_URL}-na.amazon.com", "A2Q3Y263D00KWC", "us-east-1")

    #: Amazon marketplace in Canada (CA)
    CA = (f"{BASE_URL}-na.amazon.com", "A2EUQ1WTGCTBG2", "us-east-1")

    #: Amazon marketplace in Mexico (MX)
    MX = (f"{BASE_URL}-na.amazon.com", "A1AM78C64UM0Y8", "us-east-1")

    def __init__(self, endpoint, marketplace_id, region):
        self.endpoint = endpoint
        self.marketplace_id = marketplace_id
        self.region = region

# GCS credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='../bayer-ch-ecommerce-282069d49dcf.json'

# Load Amazon Vendor Central credentials (each market different APP) from JSON file
with open('../amazon_vc_credentials.json', 'r') as file:
    credentials_list = json.load(file)   
    
# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket_name = 'ch_commerce_incoming'

def getAccessTokenViaRefreshToken(refresh_token, client_id, client_secret):
    """
    Access Token Request\n
    [REPEATED]\n
    This will be used as often as needed to make calls to the
    Advertising API\n\n

    :param refresh_token:
    :param client_id: The Login-with-Amazon (LWA) Client ID.
    :param client_secret: The Client Secret on the LWA security profile.
    :return: None, but prints the response data with the following format. \n
        { \n
            "access_token": "Atza|...", \n
            "refresh_token": "Atzr|...", \n
            "token_type": "bearer", \n
            "expires_in": 3600 \n
        } \n
    """
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post(url, data=payload)
    
    r_json = response.json()
    return r_json["access_token"]

# Function to validate JSON payload
def validate_json(payload):
    try:
        validate(instance=payload, schema=schema)
        return True
    except ValidationError as e:
        print(f"JSON validation error: {e.message}")
        return False

# Function to store JSON payload in GCS
def store_json_in_gcs(payload, bucket_name, asin, marketplace_id):
    # Generate file path with date of ingestion
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    file_name = f"global/amazon_api/catalog/dt={current_date}/market={marketplace_id}/{asin}_payload.json"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(payload), content_type='application/json')
    
    # print(f"File stored at: gs://{bucket_name}/{file_name}")

# Function to query Amazon API
def get_amazon_product_info(asin, marketplace_id, access_token, market_name):
    
    amazon_api_url = Marketplaces[market_name].endpoint
    
    # actual structure of JSON payload - what elements of the catalog to receive
    params = {
        'marketplaceIds': marketplace_id,
        'includedData': 'summaries,identifiers,vendorDetails,attributes,classifications,relationships,salesRanks,images,productTypes'
    }
    headers = {
        'Accept':'application/json',
        'x-amz-access-token':access_token
    }
    retries = 2
    while retries >= 0:
        response = requests.get(f'{amazon_api_url}/catalog/2022-04-01/items/{asin}', headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 202:
            time.sleep(5)
            retries -= 1
        elif response.status_code >= 400:
            return None
    return None

# Process each set of credentials
for credentials in credentials_list:
    marketplace_id  = credentials['marketplaceId']
    client_id       = credentials['lwa_app_id']
    client_secret   = credentials['lwa_client_secret']
    refresh_token   = credentials['refresh_token']
    market_name     = credentials['marketName']
    
    # get access token for the session 
    ACCESS_TOKEN = getAccessTokenViaRefreshToken(refresh_token, client_id, client_secret)
    # print(f'Here is the access token: {ACCESS_TOKEN}')
    # time.sleep(5)
    
    print('')
    print(f'Our market is {market_name} and marketplace ID is {marketplace_id}')        
    # Define the query to get ASINs matching the marketplace ID
    query = f"""SELECT DISTINCT asin FROM `bayer-ch-ecommerce.ch_amazon_global.v_vendor_sales_details` WHERE marketplaceIds = '{marketplace_id}'"""
    
    # Initialize BigQuery client
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    
    # show where the cursor/problem is
    current_row = 1
    
    # Total number of ASINs that are showing as Bayer associated products
    print("Market contains total of "+str(results.total_rows)+" products")
    
    # Process results
    for row in results:
        asin = row['asin']
        # which row out of the list 
        status = str(current_row) + '/' + str(results.total_rows)
        # pull the data from Amazon Catalog
        product_info = get_amazon_product_info(asin, marketplace_id, ACCESS_TOKEN, market_name)
        current_row = current_row + 1
        # gracefully use the API
        time.sleep(3)
        # validate if the JSON payload contains valid information 
        if product_info and validate_json(product_info):
            # and if good store it in Google Cloud Storage
            store_json_in_gcs(product_info, bucket_name,asin,market_name)
        else:
            print(f"Failed to get or validate product info for ASIN: {asin}, Marketplace market {market_name} and ID: {marketplace_id} - {status}.")
