from library.logger import Logger
from library.database import Database
from utils.csv_utils import create_csv, delete_csv

class ETL:
    def __init__(self, file_name: str, logger: Logger):
        self.file_name = file_name
        self.logger = logger
        self.db = Database(self.logger)

    def extract_data(self):
        try:
            self.db.connect()
            self.db.execute_query(f"""
                                SELECT * FROM
                                {self.db._source_db}.{self.file_name}""")
            data = self.db.fetch()
            column_names = [i[0] for i in self.db.cursor.description]
            create_csv(data, column_names, self.file_name)
            self.logger.log_info(f"Data fetched successfully from {self.db._source_db}.{self.file_name} into {self.file_name}.csv")
            self.db.disconnect()
        except Exception as e:
            self.db.cursor.execute("ROLLBACK")
            self.logger.log_error(f"Error fetching data: {e}")
            raise Exception(f"An error occurred while fetching data: {e}")

    def load_data(self):
        try:
            self.db.connect()
            query = f"""
                    LOAD DATA INFILE '{self.db._csv_dir_path}/{self.file_name}.csv'
                    INTO TABLE {self.db._stage_db}.{self.file_name}
                    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                    LINES TERMINATED BY '\n'
                    IGNORE 1 ROWS
                    """
            self.db.execute_query(query)
            self.db.logger.log_info(f"Data loaded into {self.db._stage_db}.{self.file_name} table.")
            delete_csv(self.file_name)
            self.db.disconnect()
        except Exception as e:
            self.db.cursor.execute("ROLLBACK")
            raise Exception(f"An unexpected error occurred: {e}")



















