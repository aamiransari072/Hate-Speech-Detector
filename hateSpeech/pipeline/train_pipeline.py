import os 
import sys
from hateSpeech.logger import logging
from hateSpeech.exception import CustomException
from hateSpeech.components.data_ingestion import DataIngestion
from hateSpeech.entity.config_entity import DataIngestionConfig
from hateSpeech.entity.artifacts_entity import DataIngestionArtifacts


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
    

    def start_data_ingestion(self)->DataIngestionArtifacts:
        logging.info("Entered start_data_ingestion method of TrainPipeline class")

        try:
            logging.info('Getting Data from gcloud storage bucket')
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            
            data_ingestion_artifacts = data_ingestion.initiate_data_ingestion()
            logging.info("Got the train and valid from GCloud Storage")
            logging.info("Exited the start_data_ingestion of Train Pipeline class")
            return data_ingestion_artifacts
        
        except Exception as e:
            raise CustomException(e,sys) from e
    
    def run_pipeline(self)->None:
        logging.info("Entered run_pipeline method of TrainPipeline class")

        try:
            data_ingestion_artifacts = self.start_data_ingestion()
            logging.info("Exited run_pipeline_method of run_pipeline class")

        except Exception as e:
            raise CustomException(e,sys) from e



