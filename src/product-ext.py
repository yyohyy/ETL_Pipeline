import os
from library.logger import Logger
from utils.etl import ETL

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
etl.extract_data()
etl.load_data()
