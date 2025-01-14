import os
import sys
from zipfile import ZipFile
from hateSpeech.logger import logging
from hateSpeech.exception import CustomException
from hateSpeech.configuration.gcloud_syncer import GCloudSync
from hateSpeech.entity.config_entity import DataIngestionConfig
from hateSpeech.entity.artifacts_entity import DataIngestionArtifacts


class DataIngestion:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        """
        :param (data_ingestion_config) : Store all the configuration fro DataIngestion Process
        """

        self.data_ingestion_config = data_ingestion_config
        self.gcloud = GCloudSync()
    

    def get_data_from_gcloud(self)->None:
        try:
            logging.info("Entered the get_data_from_gcloud method for Data Ingestion Class")
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR)
            self.gcloud.sync_folder_from_gcloud(self.data_ingestion_config.BUCKET_NAME,
                                                self.data_ingestion_config.ZIP_FILE_NAME,
                                                self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR,
                                                )
            
            logging.info("Exited the get_data_from_gclod method of Data Ingestion Class")
        
        except Exception as e:
            raise CustomException(e,sys) from e
    
    def unzip_and_clean(self):
        logging.info('Entered the umzip_and_clean method of Data Ingestion Class')
        try:
            with ZipFile(self.data_ingestion_config.ZIP_FILE_PATH,'r') as zip_ref:
                zip_ref.extractall(self.data_ingestion_config.ZIP_FILE_DIR)

            logging.info("Exited the unzip_and_clean method of Data Ingestion class")

            return self.data_ingestion_config.DATA_ARTIFACTS_DIR, self.data_ingestion_config.NEW_DATA_ARTIFACTS_DIR
        
        except Exception as e:
            raise CustomException(sys,e) from e
        

    
    def initiate_data_ingestion(self)->DataIngestionArtifacts:
        """
        Method Name : initiate_data_ingestion
        Description: This function initiates a data ingestion steps
        Output: Returns data ingestion artifacts
        On failure: Write an exception log and then raise an exception
        """

        logging.info("Entered into initiate_data_ingestion method of Data Ingestion Class")

        try:
            self.get_data_from_gcloud()
            logging.info('Feteched Data from s3 bucket')

            imbalance_data_file_path , raw_data_file_path = self.unzip_and_clean()
            logging.info('Unzipped file and split into train and valid')

            data_ingestion_artifacts = DataIngestionArtifacts(imbalance_data_file_path=imbalance_data_file_path,
                                                              raw_data_file_path=raw_data_file_path
                                                              )
            
            logging.info("Exited the initiate_data_ingestion method of Data Ingestion")

            logging.info(f"Data Ingestion Artifacts: {data_ingestion_artifacts}")

            return data_ingestion_artifacts
        
        except Exception as e:
            raise CustomException(sys,e) from e

