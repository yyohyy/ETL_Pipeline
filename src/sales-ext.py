import os
from library.logger import Logger
from library.etl import ETL
from library.database import Database

file_name = os.path.basename(__file__).split('-')[0]

logger = Logger(file_name)
etl = ETL(file_name, logger)
db = Database(logger)

def sales_etl():
    try:
        etl.extract_data()
        etl.load_data()
        db.connect()
        truncate_temp = f"""
        TRUNCATE TABLE {db._temp_db}.TMP_SALES
        """
        db.execute_query(truncate_temp)
        temp_query = f"""
            INSERT INTO {db._temp_db}.TMP_SALES
            (SLS_ID, STORE_ID, PDT_ID, CUSTOMER_ID, TRANSACTION_TIME, QTY, AMT, DSCNT)
            SELECT
            ID AS SLS_ID,
            STORE_ID AS STORE_ID,
            PRODUCT_ID AS PDT_ID,
            CUSTOMER_ID AS CUSTOMER_ID,
            TRANSACTION_TIME,
            QUANTITY AS QTY,
            AMOUNT AS AMT,
            DISCOUNT AS DSCNT
            FROM {db._stage_db}.SALES;
        """

        db.execute_query(temp_query)
        truncate_target = f"""
            TRUNCATE TABLE {db._target_db}.F_RETAIL_SLS_T
            """
        db.execute_query(truncate_target)
        target_query = f"""
        INSERT INTO {db._target_db}.F_RETAIL_SLS_T 
        (SLS_ID, LOCN_KY, DT_KY, PDT_KY, CUSTOMER_KY, TRANSACTION_TIME, QTY, AMT, DSCNT, ROW_INSRT_TMS, ROW_UPDT_TMS)
        SELECT 
          SLS_ID,   
          LOCN_KY,         
          CA.DAY_KEY AS DT_KY,
          PDT_KY,                  
          CUSTOMER_KY,             
          TRANSACTION_TIME,        
          QTY,         
          AMT,           
          DSCNT,       
          CURRENT_TIMESTAMP,         
          CURRENT_TIMESTAMP          
        FROM {db._temp_db}.TMP_SALES S
        LEFT JOIN {db._target_db}.D_RETAIL_PDT_T P ON S.Pdt_ID = P.PDT_ID  
        LEFT JOIN {db._target_db}.D_RETAIL_CUSTOMER_T C ON S.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN {db._target_db}.D_RETAIL_LOCN_T L ON S.STORE_ID = L.LOCN_ID
        LEFT JOIN {db._target_db}.DIM_CALENDAR CA ON DATE(S.TRANSACTION_TIME) = CA.DATE;
        """
        db.execute_query(target_query)
        db.disconnect()
    except Exception as e:
        logger.log_error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    sales_etl()