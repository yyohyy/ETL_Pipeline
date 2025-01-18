import mysql.connector
from library.logger import Logger
from library.variables import Variables

class Database:
    _config = Variables("DATABASE").get_variable()
    def __init__(self, logger: Logger):
        self.connection = None
        self.cursor = None
        self.logger = logger
        self.config = self._config

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor(buffered=True)
            self.logger.log_info("Connected to MYSQL database.")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error connecting to MYSQL: {e}")

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(query, params or ())
            self.connection.commit()
            self.logger.log_info(f"Query executed: {query}")
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error executing query {e}")

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            self.logger.log_info("Disconnected from database")

    def fetch(self):
        try:
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            self.logger.log_error(f"Error fetching data: {e}")
            return None


