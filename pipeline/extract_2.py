import luigi
from datetime import datetime
import logging
import time
import pandas as pd
import os
import traceback

from pipeline.utility.db_conn import db_connection
from pipeline.utility.read_sql import read_sql_file
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_LOG = os.getenv("DIR_LOG")


class Extract(luigi.Task):
    # Define tables to be extracted from db sources
    tables_to_extract = [
        'public.geolocation', 
        'public.product_category_name_translation', 
        'public.sellers', 
        'public.customers', 
        'public.products', 
        'public.orders',
        'public.order_items',
        'public.order_payments',
        'public.order_reviews'
    ]
    
    def requires(self):
        return None

    def run(self):
        try:
            # Ensure required directories exist
            os.makedirs(DIR_TEMP_DATA, exist_ok=True)
            os.makedirs(DIR_TEMP_LOG, exist_ok=True)

            # Configure logging safely
            if not logging.getLogger().hasHandlers():
                logging.basicConfig(
                    filename=f'{DIR_TEMP_LOG}/logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s'
                )

            # Connect to source DB
            src_engine, _ = db_connection()

            # Read base query
            extract_query = read_sql_file(
                file_path=f'{DIR_EXTRACT_QUERY}/extract-all-tables.sql'
            )

            start_time = time.time()
            logging.info("============== STARTING EXTRACT DATA ==============")

            for table_name in self.tables_to_extract:
                try:
                    # Read from DB
                    df = pd.read_sql_query(extract_query.format(table_name=table_name), src_engine)

                    # Save to CSV
                    df.to_csv(f"{DIR_TEMP_DATA}/{table_name}.csv", index=False)

                    logging.info(f"EXTRACT '{table_name}' - SUCCESS.")

                except Exception as e:
                    logging.error(f"EXTRACT '{table_name}' - FAILED. Reason: {str(e)}")
                    logging.error(traceback.format_exc())
                    raise

            logging.info("Extract All Tables From Sources - SUCCESS")

            # Calculate duration
            execution_time = time.time() - start_time

            # Write summary
            pd.DataFrame([{
                'timestamp': datetime.now(),
                'task': 'Extract',
                'status': 'Success',
                'execution_time': execution_time
            }]).to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index=False)

        except Exception as e:
            logging.error(f"Extract All Tables From Sources - FAILED. Reason: {str(e)}")
            logging.error(traceback.format_exc())

            # Write failure summary
            pd.DataFrame([{
                'timestamp': datetime.now(),
                'task': 'Extract',
                'status': 'Failed',
                'execution_time': 0
            }]).to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index=False)

            raise Exception("FAILED to execute EXTRACT TASK")

        finally:
            logging.info("============== ENDING EXTRACT DATA ==============")

    def output(self):
        outputs = [
            luigi.LocalTarget(f'{DIR_TEMP_DATA}/{table_name}.csv')
            for table_name in self.tables_to_extract
        ]
        outputs.append(luigi.LocalTarget(f'{DIR_TEMP_DATA}/extract-summary.csv'))
        outputs.append(luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'))
        return outputs