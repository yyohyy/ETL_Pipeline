import os
from library.database import Database
from library.logger import Logger
from library.etl import ETL

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def country_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_COUNTRY
        """
        db.execute_query(truncate_temp)
        temp_query= f"""    
        INSERT INTO {db._temp_db}.TMP_COUNTRY (CNTRY_ID, CNTRY_DESC)
        SELECT
        ID AS CNTRY_ID,
        COUNTRY_DESC AS CNTRY_DESC
        FROM {db._stage_db}.country    
        """
        db.execute_query(temp_query)
        truncate_target = f"""
        TRUNCATE TABLE {db._target_db}.D_RETAIL_CNTRY_T
        """
        db.execute_query(truncate_target)
        target_query = f"""
        INSERT INTO {db._target_db}.D_RETAIL_CNTRY_T (CNTRY_ID, CNTRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
        SELECT 
            CNTRY_ID,
            CNTRY_DESC,
            CURRENT_TIMESTAMP as row_insrt_tms,
            CURRENT_TIMESTAMP as row_updt_tms
        FROM {db._temp_db}.TMP_COUNTRY TMP
        WHERE TMP.CNTRY_ID;    
        """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        db.disconnect()
        logger.log_error(e)

if __name__ == "__main__":
    country_etl()