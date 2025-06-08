import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime

import os
import sys

sys.path.append(r"C:\Users\LENOVO THINKPAD\Documents\Github\Pacmann---Data-Storage_-Warehouse--Mart---Lake\mentoring 2\olist-dwh-project")

from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.utility.db_conn import db_connection
from pipeline.utility.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TRANSFORM_QUERY = os.getenv("DIR_TRANSFORM_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Transform(luigi.Task):
    
    def requires(self):
        return Load()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            
            # Read transform query to final schema
            dim_geolocation_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-dim_geolocation.sql'
            )
            
            dim_product_category_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-dim_product_category.sql'
            )
            
            dim_sellers_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-dim_sellers.sql'
            )
            
            dim_customers_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-dim_customers.sql'
            )
            
            dim_products_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-dim_products.sql'
            )
            
            fact_orders_comprehensive_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-fact_orders_comprehensive.sql'
            )
            
            fact_reviews_sentiment_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-fact_reviews_sentiment.sql'
            )
            
            fact_daily_periodicals_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-fact_daily_periodical_snapshot_order_trends.sql'
            )
            
            fact_accumulating_snapshot_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/transform-dwh-fact_accumulating_snapshot_logistic_performance.sql'
            )
            
            logging.info("Read Transform Query - SUCCESS")
            
        except Exception:
            logging.error("Read Transform Query - FAILED")
            raise Exception("Failed to read Transform Query")        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for transform tables
        start_time = time.time()
        logging.info("==================================STARTING TRANSFROM DATA=======================================")  
               
        # Transform to dimensions tables
        try:
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            
            # Transform to `dwh` tables
            
            query = sqlalchemy.text(dim_geolocation_query)
            session.execute(query)
            logging.info("Transform to 'dwh.dim_geolocation' - SUCCESS")
            
            query = sqlalchemy.text(dim_product_category_query)
            session.execute(query)
            logging.info("Transform to 'dwh.dim_product_category' - SUCCESS")
            
            query = sqlalchemy.text(dim_sellers_query)
            session.execute(query)
            logging.info("Transform to 'dwh.dim_sellers' - SUCCESS")
            
            query = sqlalchemy.text(dim_customers_query)
            session.execute(query)
            logging.info("Transform to 'dwh.dim_customers' - SUCCESS")
        
            query = sqlalchemy.text(dim_products_query)
            session.execute(query)
            logging.info("Transform to 'dwh.dim_products' - SUCCESS")
            
            query = sqlalchemy.text(fact_orders_comprehensive_query)
            session.execute(query)
            logging.info("Transform to 'dwh.fact_orders_comprehensive' - SUCCESS")
            
            query = sqlalchemy.text(fact_reviews_sentiment_query)
            session.execute(query)
            logging.info("Transform to 'dwh.fact_reviews_sentiment' - SUCCESS")
            
            query = sqlalchemy.text(fact_daily_periodicals_query)
            session.execute(query)
            logging.info("Transform to 'dwh.fact_daily_periodicals_order_trends' - SUCCESS")
            
            query = sqlalchemy.text(fact_accumulating_snapshot_query)
            session.execute(query)
            logging.info("Transform to 'dwh.fact_accumulating_snapshot_logistic_performance' - SUCCESS")
            
            # Commit transaction
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Transform to All Dimensions and Fact Tables - SUCCESS")
            
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
        except Exception:
            logging.error(f"Transform to All Dimensions and Fact Tables - FAILED")
        
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
            logging.error("Transform Tables - FAILED")
            raise Exception('Failed Transforming Tables')   
        
        logging.info("==================================ENDING TRANSFROM DATA=======================================") 

    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/transform-summary.csv')]