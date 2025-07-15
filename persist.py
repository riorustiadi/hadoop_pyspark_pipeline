import pyspark
from pyspark.sql import SparkSession
import logging
import logging.config
import sys
#import psycopg2
import configparser

logger = logging.getLogger("Persist")
config = configparser.ConfigParser()
config.read("resources/pipeline.ini")
target_table = config.get("db_configs", "target_pg_table")

class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self,df):
        try:
            logger.info("Persisting")
         # menyimpan dataframe ke HDFS: local home directory
            #df.write.option("header", "true").csv("transformed_retailstore")
            #df.coalesce(1).write.option("header", "true").csv("transformed_retailstore")

         # menyimpan dataframe ke PostgreSQL menggunakan JDBC   
            df.write \
                .mode("overwrite") \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/postgres") \
                .option("dbtable", "target_table") \
                .option("user", "postgres") \
                .option("password", "admin") \
                .save()
            logger.info("Target table is: " +str(target_table))
            logger.info("Transformed data has been saved")
        except Exception as exp:
            logger.error("An error occurred while persisting data >"+ str(exp))
            # store in database table
            # send email notification
            raise Exception("HDFS directory already exists")
        
 # menambahkan record ke tabel course_catalog di PostgreSQL menggunakan Psycopg2    
    # def insert_into_pg(self):
    #     connection = psycopg2.connect(user="postgres", password="12183rio", host="localhost", port="5432", database="postgres")
    #     cursor = connection.cursor()
    #     insert_query = "insert into course_schema.course_catalog (course_id, course_name, author_name, course_section, creation_date) VALUES (%s, %s, %s, %s, %s)"
    #     insert_tuple = (3, "Machine Learning", "Datalogi", "{}", "2020-10-20")
    #     cursor.execute(insert_query, insert_tuple)
    #     cursor.close()
    #     connection.commit()
    

