import os
from library.database import Database
from library.logger import Logger
from library.etl import ETL

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def category_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_CATEGORY
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_CATEGORY (CTGRY_ID, CTGRY_DESC)
            SELECT
            ID AS CTGRY_ID,
            CATEGORY_DESC AS CTGRY_DESC
            FROM {db._stage_db}.CATEGORY;
            """
        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.D_RETAIL_CTGRY_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
            INSERT INTO {db._target_db}.D_RETAIL_CTGRY_T (CTGRY_ID, CTGRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
            SELECT 
                CTGRY_ID, 
                CTGRY_DESC,
                CURRENT_TIMESTAMP as row_insrt_tms,
                CURRENT_TIMESTAMP as row_updt_tms
            FROM {db._temp_db}.TMP_CATEGORY TMP
            WHERE TMP.CTGRY_ID;    
            """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    category_etl()