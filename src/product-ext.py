import os
from library.database import Database
from library.logger import Logger
from library.etl import ETL

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def product_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_PRODUCT
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_PRODUCT (PDT_ID, SUB_CTGRY_ID, PDT_DESC)
            SELECT
                ID AS PDT_ID,
                SUBCATEGORY_ID AS SUB_CTGRY_ID,
                PRODUCT_DESC AS PDT_DESC
            FROM {db._stage_db}.Product; 
        """
        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.D_RETAIL_PDT_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
            INSERT INTO {db._target_db}.D_RETAIL_PDT_T (PDT_ID, SUB_CTGRY_KY, CTGRY_KY, PDT_DESC, Row_INSRT_TMS, Row_UPDT_TMS)
            SELECT 
                PDT_ID,
                SUB_CTGRY_KY,   
                CTGRY_KY,     
                PDT_DESC,
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP
            FROM {db._temp_db}.TMP_PRODUCT TMP
            LEFT JOIN {db._target_db}.D_RETAIL_SUB_CTGRY_T S
            ON TMP.SUB_CTGRY_ID = S.SUB_CTGRY_ID
            WHERE TMP.PDT_ID;    
            """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(e)

if __name__ == "__main__":
    product_etl()