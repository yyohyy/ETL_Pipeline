import os
from library.logger import Logger
from library.etl import ETL
from library.database import Database

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def customer_etl():
    try:

        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_CUSTOMER
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_CUSTOMER
            (CUSTOMER_ID, CUSTOMER_FST_NM, CUSTOMER_MID_NM, CUSTOMER_LST_NM, CUSTOMER_ADDR)
            SELECT
            ID AS CUSTOMER_ID,
            CUSTOMER_FIRST_NAME AS CUSTOMER_FST_NM,
            CUSTOMER_MIDDLE_NAME AS CUSTOMER_MID_NM,
            CUSTOMER_LAST_NAME AS CUSTOMER_LST_NM,
            CUSTOMER_ADDRESS AS CUSTOMER_ADDR
            FROM {db._stage_db}.CUSTOMER;
        """
        db.execute_query(temp_query)
        truncate_target = f"""
        TRUNCATE TABLE {db._target_db}.TMP_TARGET
        """
        db.execute_query(truncate_target)
        target_query = f"""
        INSERT INTO {db._target_db}.D_RETAIL_CUSTOMER_T (CUSTOMER_ID, CUSTOMER_FST_NM, CUSTOMER_MID_NM, CUSTOMER_LST_NM, CUSTOMER_ADDR, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
          CUSTOMER_ID, 
          CUSTOMER_FST_NM, 
          CUSTOMER_MID_NM, 
          CUSTOMER_LST_NM, 
          CUSTOMER_ADDR,
          CURRENT_TIMESTAMP, 
          CURRENT_TIMESTAMP
        FROM {db._temp_db}.TMP_CUSTOMER TMP
        WHERE TMP.CUSTOMER_ID;    
        """
        db.execute_query(target_query)
        db.disconnect()

    except Exception as e:
        logger.log_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    customer_etl()