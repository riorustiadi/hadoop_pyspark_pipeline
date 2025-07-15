import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
logger = logging.getLogger("Transform")

class Transform:
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        logger.info("Transforming")
        
     #drop all rows with null values
        #df1 = df.na.drop()
        
     # ubah kosong menjadi unknown atau null
        df1 = df.na.fill("Unknown", ["author_name"]).replace('', 'Unknown', subset=['author_name'])
        df2 = df1.na.fill("0", ["no_of_reviews"]).replace('', '0', subset=['no_of_reviews'])
        #df1 = df.replace("", "Unknown", subset="author_name")
        #df2 = df1.replace("", "0", subset=["no_of_reviews"])
        
        logger.info("data has been transformed")
        
        return df2
    