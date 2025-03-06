import os
from library.logger import Logger
from library.etl import ETL
from library.database import Database

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def subcategory_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_SUBCATEGORY
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_SUBCATEGORY
            (SUB_CTGRY_ID, SUB_CTGRY_DESC, CTGRY_ID)
            SELECT
            ID AS SUB_CTGRY_ID,
            SUBCATEGORY_DESC AS SUB_CTGRY_DESC,
            CATEGORY_ID AS CTGRY_ID        
            FROM {db._stage_db}.SUBCATEGORY;
        """
        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.D_RETAIL_SUB_CTGRY_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
            INSERT INTO {db._target_db}.D_RETAIL_SUB_CTGRY_T (SUB_CTGRY_ID, CTGRY_KY, SUB_CTGRY_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
            SELECT 
                SUB_CTGRY_ID,
                CTGRY_KY, 
                SUB_CTGRY_DESC,
                CURRENT_TIMESTAMP as row_insrt_tms,
                CURRENT_TIMESTAMP as row_updt_tms
            FROM {db._temp_db}.TMP_SUBCATEGORY TMP
            LEFT JOIN {db._target_db}.D_RETAIL_CTGRY_T C
            ON TMP.CTGRY_ID = C.CTGRY_ID
            WHERE TMP.SUB_CTGRY_ID;    
            """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    subcategory_etl()