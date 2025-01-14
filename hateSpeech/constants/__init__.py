import os 
from datetime import datetime

# Common Constants 

TIMESTAMP: str=datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
ARTIFACTS_DIR = os.path.join('artifacts',TIMESTAMP)
BUCKET_NAME = 'hate-speech072'
ZIP_FILE_NAME = 'dataset.zip'
LABEL = "label"
TWEET = 'tweet'
MODEL_NAME = 'model.h5'
APP_HOST = "0.0.0.0"
APP_PORT = 8080

# Data Ingestion Constant
DATA_INGESTION_ARTIFACTS_DIR = 'DataIngestionArtifacts'
DATA_INGESTION_IMBALANCE_DATA_DIR = 'imbalance_data.csv'
DATA_INGESTION_RAW_DATA_DIR = 'raw_data.csv'
