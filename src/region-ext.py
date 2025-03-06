import os
from library.logger import Logger
from library.etl import ETL
from library.database import Database

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def region_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_REGION
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_REGION
            (RGN_ID, CNTRY_ID, RGN_DESC)
            SELECT
            ID AS RGN_ID,
            COUNTRY_ID AS CNTRY_ID,
            REGION_DESC AS RGN_DESC
            FROM {db._stage_db}.REGION;
        """
        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.D_RETAIL_RGN_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
        INSERT INTO {db._target_db}.D_RETAIL_RGN_T (RGN_ID, CNTRY_KY, RGN_DESC, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
            RGN_ID, 
            CNTRY_KY, 
            RGN_DESC, 
            CURRENT_TIMESTAMP as row_insrt_tms, 
            CURRENT_TIMESTAMP as row_updt_tms
        FROM {db._temp_db}.TMP_REGION TMP
        LEFT JOIN {db._target_db}.D_RETAIL_CNTRY_T C
        ON TMP.CNTRY_ID = C.CNTRY_ID
        WHERE TMP.RGN_ID;
        """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    region_etl()