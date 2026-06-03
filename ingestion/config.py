import os
from dotenv import load_dotenv

load_dotenv()


GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
if GOOGLE_APPLICATION_CREDENTIALS is None:
    raise ValueError('Google Application Credentials was not read from the .env file')

SOCRATA_APP_TOKEN  = os.getenv('SOCRATA_APP_TOKEN', None)
if SOCRATA_APP_TOKEN is None:
    raise ValueError('Socrata App Token was not read from the .env file')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', None)
if GCP_PROJECT_ID is None:
    raise ValueError('Google Project ID was not read from the .env file')

