import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
#import psycopg2
import pandas
import pandas.io.sql as psql
logger = logging.getLogger("Ingest")

class Ingestion:
    def __init__(self, spark):
        self.spark = spark
 
 # Ingest data from hive table      
    def ingest_data(self):        
        logger.info("Ingesting from hive")
        hive_df = self.spark.sql("select * from coursedb.course_table")
        logger.info("DataFrame created")
        return hive_df

 # Ingest data CSV from Amazon S3 Bucket
   #  def ingest_data_from_s3(self):
   #      logger.info("Ingesting CSV from S3")
   #      csv_df = self.spark.read.csv("s3a://<your-bucket-name>/<your-file.csv>", header=True, inferSchema=True)
   #      logger.info("DataFrame created")
   #      return csv_df

    
 # Ingest data from a CSV file       
    # def ingest_data(self):        
    #     logger.info("Ingesting from csv")                
    #     csv_df = self.spark.read.csv("retailstore.csv", header=True, inferSchema=True)
    #     logger.info("DataFrame created")
    #     return csv_df
    
 # Ingest data from PostgreSQL using Psycopg2 and convert to Spark DataFrame
    # def read_from_pg(self):
    #     connection = psycopg2.connect(user="postgres", password="admin", host="localhost", port="5432", database="postgres")
    #     cursor = connection.cursor()
    #     sql_query = "SELECT * FROM course_schema.course_catalog"
    #     pdDF = psql.read_sql_query(sql_query, connection)
    #     sparkDF = self.spark.createDataFrame(pdDF)
    #     sparkDF.show()

 # Ingest data from PostgreSQL using JBDC
    # def read_from_pg_using_jbdc_driver(self):
    #     jbdcDF = self.spark.read \
    #         .format("jdbc") \
    #         .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    #         .option("dbtable", "course_schema.course_catalog") \
    #         .option("user", "postgres") \
    #         .option("password", "admin") \
    #         .load()
            
    #     jbdcDF.show()