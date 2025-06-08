import luigi
import logging
import pandas as pd
import time
import sqlalchemy

import os
#import sys

#sys.path.append(r"C:\Users\LENOVO THINKPAD\Documents\Github\Pacmann---Data-Storage_-Warehouse--Mart---Lake\mentoring 2\olist-dwh-project")

from datetime import datetime

from pipeline.extract import Extract
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
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

# read sql query files to load from `public` to `stg` (only read, not execute)
# reading csv file into df
# establishing connection to dwh_engine
# truncate / delete the content of the public schema
# do `to_sql()` from the df, to the `public` schema
# execute the queries to load from `public` to `stg` schema

class Load(luigi.Task):   
    
    def requires(self):
        return Extract()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read query to truncate public schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-public-truncate_tables.sql'
            )
            
            # Read load query to staging schema
            geolocation_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-geolocation.sql'
            )
            
            product_category_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-product_category_name_translation.sql'
            )
            
            sellers_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-sellers.sql'
            )
            
            customers_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-customers.sql'
            )
            
            products_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-products.sql'
            )
            
            orders_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-orders.sql'
            )  
            
            order_items_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-order_items.sql'
            )  
            
            order_payments_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-order_payments.sql'
            )
            
            order_reviews_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/load-stg-order_reviews.sql'
            )                                        
            
            logging.info("Read Load Query - SUCCESS")
            
        except Exception:
            logging.error("Read Load Query - FAILED")
            raise Exception("Failed to read Load Query")

        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read Data to be load
        try:
            # Read csv
            geolocation = pd.read_csv(self.input()[0].path)
            product_category = pd.read_csv(self.input()[1].path)
            sellers = pd.read_csv(self.input()[2].path)
            customers = pd.read_csv(self.input()[3].path)
            products = pd.read_csv(self.input()[4].path)
            orders = pd.read_csv(self.input()[5].path)
            order_items = pd.read_csv(self.input()[6].path)
            order_payments = pd.read_csv(self.input()[7].path)
            order_reviews = pd.read_csv(self.input()[8].path)
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception:
            logging.error(f"Read Extracted Data  - FAILED")
            raise Exception("Failed to Read Extracted Data")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to olist-dwh - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to olist-dwh - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Truncate all tables in public schema before load
        # This purpose to avoid errors because duplicate key value violates unique constraint
        try:            
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()

            # Execute each query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
                
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Truncate `public` Schema in olist-dwh - SUCCESS")
        
        except Exception:
            logging.error(f"Truncate `public` Schema in olist-dwh - FAILED")
            
            raise Exception("Failed to Truncate `public` schema in olist-dwh")
        
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for loading tables
        start_time = time.time()  
        logging.info("==================================STARTING LOAD DATA=======================================")
        # Load to tables
        try:
            
            try:
                # Load to public schema   
                geolocation.to_sql('geolocation', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'public')
                
                product_category.to_sql('product_category_name_translation', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'public')
                
                sellers.to_sql('sellers', 
                                con = dwh_engine, 
                                if_exists = 'append', 
                                index = False, 
                                schema = 'public')
                
                customers.to_sql('customers', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'public')
                
                products.to_sql('products', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'public')
                
                orders.to_sql('orders', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'public')
                
                order_items.to_sql('order_items', 
                                   con = dwh_engine, 
                                   if_exists = 'append', 
                                   index = False, 
                                   schema= 'public')
                
                order_payments.to_sql('order_payments',
                                      con = dwh_engine,
                                      if_exists= 'append',
                                      index=False,
                                      schema='public')
                
                order_reviews.to_sql('order_reviews',
                                     con = dwh_engine,
                                     if_exists= 'append',
                                     index=False,
                                     schema='public')
                
                logging.info(f"LOAD All Tables To olist-dwh-public - SUCCESS")
                
            except Exception:
                logging.error(f"LOAD All Tables To olist-dwh-public - FAILED")
                raise Exception('Failed Load Tables To olist-dwh-public')
            
            
            #----------------------------------------------------------------------------------------------------------------------------------------
            # Load from public schema to staging schema
            try:
                # List query
                load_stg_queries = [geolocation_query, product_category_query,
                                    sellers_query, customers_query, products_query,
                                    orders_query, order_items_query, order_payments_query, order_reviews_query]
                
                # Create session
                Session = sessionmaker(bind = dwh_engine)
                session = Session()

                # Execute each query
                for query in load_stg_queries:
                    query = sqlalchemy.text(query)
                    session.execute(query)
                    
                session.commit()
                
                # Close session
                session.close()
                
                logging.info("LOAD All Tables To DWH-Staging - SUCCESS")
                
            except Exception:
                logging.error("LOAD All Tables To DWH-Staging - FAILED")
                raise Exception('Failed Load Tables To DWH-Staging')
        
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
                        
        #----------------------------------------------------------------------------------------------------------------------------------------
        except Exception:
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            logging.error("LOAD All Tables To DWH - FAILED")
            raise Exception('Failed Load Tables To DWH')   
        
        logging.info("==================================ENDING LOAD DATA=======================================")
        
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]