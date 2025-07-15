import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import ingest
import transfom
import persist
import logging
import logging.config
import sys

logging.config.fileConfig("resources/configs/logging.conf")

class Pipeline:
           
    def run_pipeline(self):
        # This method orchestrates the entire data pipeline
        try:            
            logging.info("run_pipeline method started")        
            ingest_process = ingest.Ingestion(self.spark)
            #ingest_process.read_from_pg()  # Read from PostgreSQL using Psycopg2
            #ingest_process.read_from_pg_using_jbdc_driver()  # Read from PostgreSQL using JDBC
            logging.info("Ingestion process started")
            df = ingest_process.ingest_data()
            df.show()
        
            transfom_process = transfom.Transform(self.spark)
            transformed_df = transfom_process.transform_data(df)
            transformed_df.show()
        
            persist_process = persist.Persist(self.spark)
            #persist_process.insert_into_pg()  # Insert into PostgreSQL using Psycopg2
            persist_process.persist_data(transformed_df)        
            logging.info("run_pipeline method ended")
        except Exception as exp:
            logging.error("An error occured while running the pipeline: " + str(exp))
            sys.exit(1)
        return

 # Spark session ingestion form Amazon S3 Bucket 
    def create_spark_session(self):
        self.spark = pyspark.sql.SparkSession.builder \
            .appName("DataPipeline") \
            .config("spark.hadoop.fs.s3a.access.key", "***") \
            .config("spark.hadoop.fs.s3a.secret.key", "***") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .enableHiveSupport() \
            .getOrCreate()

 # Default Spark session creation with Hive support
    # def create_spark_session(self):    
    #     self.spark = pyspark.sql.SparkSession.builder \
    #         .appName("DataPipeline") \
    #         .enableHiveSupport() \
    #         .getOrCreate()
     
 # Spark session ingestion from PostgreSQL using JDBC
    # def create_spark_session(self):    
    #     self.spark = pyspark.sql.SparkSession.builder \
    #         .appName("DataPipeline") \
    #         .config("spark.driver.extraClassPath", "resources/postgresql-42.7.7.jar") \
    #         .enableHiveSupport() \
    #         .getOrCreate()
    
 # Membuat tabel Hive untuk testing ingestion
    # def create_hive_table(self):
    #     self.spark.sql("create database if not exists coursedb")
    #     self.spark.sql("create table if not exists coursedb.course_table (course_id string,course_name string,author_name string,no_of_reviews string)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (1,'Java','FutureX',45)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (2,'Java','FutureXSkill',56)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (3,'Big Data','Future',100)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (4,'Linux','Future',100)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (5,'Microservices','Future',100)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (6,'CMS','',100)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (7,'Python','FutureX','')")
    #     self.spark.sql("insert into coursedb.course_table VALUES (8,'CMS','Future',56)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (9,'Dot Net','FutureXSkill',34)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (10,'Ansible','FutureX',123)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (11,'Jenkins','Future',32)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (12,'Chef','FutureX',121)")
    #     self.spark.sql("insert into coursedb.course_table VALUES (13,'Go Lang','',105)")
    #     #Treat empty strings as null
    #     self.spark.sql("alter table coursedb.course_table set tblproperties('serialization.null.format'='')")
      
        
if __name__ == "__main__":  
    
    logging.info("Application started") 
    pipeline = Pipeline()
    pipeline.create_spark_session()
    logging.info("Spark session created")
       
    #pipeline.create_hive_table()  # Jika perlu: membuat Hive Table dari kode spark.sql di atas
    #logging.info("Hive table created")
    
    pipeline.run_pipeline()    
    logging.info("Pipeline executed")
    