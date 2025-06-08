import luigi
import sentry_sdk
import pandas as pd
import os

from pipeline.extract_2 import Extract
from pipeline.load_2 import Load
from pipeline.transform_2 import Transform
from pipeline.utility.concat_dataframe import concat_dataframes
from pipeline.utility.copy_log import copy_log
from pipeline.utility.delete_temp_data import delete_temp

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read env variables
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Track the error using sentry
sentry_sdk.init(
    dsn = f"{SENTRY_DSN}"
)

# Execute the functions when the script is run
if __name__ == "__main__":
    # Build the task
    luigi.build([Extract(),
                 Load(),
                 Transform()])
    
    # Concat temp extract summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    )
    
    # Concat temp load summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/load-summary.csv')
    )
    
    # Concat temp load summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/transform-summary.csv')
    )
    
    # Append log from temp to final log
    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'
    )
    
    # Delete temp data
    delete_temp(
        directory = f'{DIR_TEMP_DATA}'
    )
    
    # Delete temp log
    delete_temp(
        directory = f'{DIR_TEMP_LOG}'
    )