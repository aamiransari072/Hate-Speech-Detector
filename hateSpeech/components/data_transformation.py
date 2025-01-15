import os 
import sys
import string 
import re
import pandas as pd 
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
from sklearn.model_selection import train_test_split
from hateSpeech.exception import CustomException
from hateSpeech.logger import logging
from hateSpeech.entity.config_entity import DataTransformationConfig
from hateSpeech.entity.artifacts_entity import DataTransformationArtifacts , DataIngestionArtifacts


class DataTransformation:
    def __init__(self,data_transformation_config:DataTransformationConfig,data_ingestion_artifacts:DataIngestionArtifacts):
        self.data_transformation_config = data_transformation_config
        self.data_ingestion_artifacts = data_ingestion_artifacts
    

    def imbalance_data_cleaning(self):
        try:
            logging.info("Entered into imbalance_data_cleaning method of DataTransformation Class")
            print(self.data_ingestion_artifacts.imbalance_data_file_path)
            imbalance_data = pd.read_csv(self.data_ingestion_artifacts.imbalance_data_file_path)
            imbalance_data.drop(self.data_transformation_config.ID,axis=self.data_transformation_config.AXIS,
                                inplace=self.data_transformation_config.INPLACE)
            logging.info(f"Exited the imbalance_data_cleaning method and return imbalance data {imbalance_data}")
            return imbalance_data
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def raw_data_cleaning(self):
        try:
            logging.info("Entered raw_data_cleaninng method of DataTransformation Class")
            raw_data = pd.read_csv(self.data_ingestion_artifacts.raw_data_file_path)
            raw_data.drop(self.data_transformation_config.DROP_COLUMNS,axis=self.data_transformation_config.AXIS,
                          inplace=self.data_transformation_config.INPLACE)
            
            raw_data[raw_data[self.data_transformation_config.CLASS]==0][self.data_transformation_config.CLASS]=1
            
            raw_data[self.data_transformation_config.CLASS].replace({0:1},inplace=True)

            raw_data[self.data_transformation_config.CLASS].replace({2:0},inplace=True)

            raw_data.rename(columns={self.data_transformation_config.CLASS:self.data_transformation_config.LABEL},inplace=True)

            logging.info(f"Exited the raw_data_cleaning method and retured raw data {raw_data}")
            return raw_data
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def concat_datafrmae(self):
        try:
            logging.info("Entered into Concat_dataframe method of Data Transformation Class")

            frame = [self.raw_data_cleaning(),self.imbalance_data_cleaning()]
            df = pd.concat(frame)
            print(df.head())
            logging.info(f"Retured the concatinated datafrmae {df}")
            return df
        
        except Exception as e:
            raise CustomException(e,sys) from e
        
    
    def concat_data_cleaning(self,words):
        try:
            logging.info("Entered into concat_data_cleaning method of Data Transformation Class")

            stemmer = nltk.SnowballStemmer("english")
            stopword = set(stopwords.words('english'))
            words = str(words).lower()
            words = re.sub('\[.*?\]', '', words)
            words = re.sub('https?://\S+|www\.\S+', '', words)
            words = re.sub('<.*?>+', '', words)
            words = re.sub('[%s]' % re.escape(string.punctuation), '', words)
            words = re.sub('\n', '', words)
            words = re.sub('\w*\d\w*', '', words)
            words = [word for word in words.split(' ') if words not in stopword]
            words=" ".join(words)
            words = [stemmer.stem(word) for word in words.split(' ')]
            words=" ".join(words)
            logging.info("Exited the concat_data_cleaning function")
            return words
        
        except Exception as e:
            raise CustomException(e,sys) from e
    

    def initiate_data_transformation(self)->DataTransformationArtifacts:
        try:
            logging.info("Entered into initiate_data_transformation method of Data Transformation class")
            self.imbalance_data_cleaning()
            self.raw_data_cleaning()
            df = self.concat_datafrmae()
            df[self.data_transformation_config.TWEET] = df[self.data_transformation_config.TWEET].apply(self.concat_data_cleaning)

            os.makedirs(self.data_transformation_config.DATA_TRANSFORMATION_ARTIFACTS_DIR,exist_ok=True)
            df.to_csv(self.data_transformation_config.TRANSFORMED_FILE_PATH,index=False,header=True)

            data_transformation_artifacts = DataTransformationArtifacts(
                transformed_file_path=self.data_transformation_config.TRANSFORMED_FILE_PATH
            )
            logging.info("Returning the DataTransformationArtifacts")
            return DataTransformationArtifacts
        
        except Exception as e:
            raise CustomException(e,sys) from e
    

