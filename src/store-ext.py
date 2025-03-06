import os
from library.database import Database
from library.logger import Logger
from library.etl import ETL

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def store_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_STORE
        """
        temp_query = f"""
        INSERT INTO {db._temp_db}.TMP_STORE (STORE_ID, RGN_ID, STORE_DESC)
        SELECT
            ID AS STORE_ID,
            REGION_ID AS RGN_ID,
            STORE_DESC
        FROM {db._stage_db}.STORE;
        """
        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.D_RETAIL_LOCN_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
            INSERT INTO {db._target_db}.D_RETAIL_LOCN_T (LOCN_ID, RGN_KY, CNTRY_KY, LOCN_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
            SELECT 
                STORE_ID AS LOCN_ID,
                RGN_KY,
                CNTRY_KY, 
                STORE_DESC AS LOCN_DESC,
                CURRENT_TIMESTAMP as row_insrt_tms,
                CURRENT_TIMESTAMP as row_updt_tms
            FROM {db._temp_db}.TMP_STORE TMP
            LEFT JOIN {db._target_db}.D_RETAIL_RGN_T R
            ON TMP.RGN_ID = R.RGN_ID
            WHERE TMP.STORE_ID;    
            """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(e)

if __name__ == "__main__":
    store_etl()

