import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from sqlalchemy import create_engine, text
from datetime import datetime
from pipeline.extract import Extract
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(Luigi.task):
    def requires(self):
        return Extract()
    
    def run(self):
        
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
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
        
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        # truncating `public schema`
        try:
            truncate_query = 