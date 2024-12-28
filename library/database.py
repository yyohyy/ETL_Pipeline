import mysql.connector
from logger import Logger
from variables import Variables
from datetime import datetime

class Database():
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.log_file_name = "database_" + datetime.now().strftime("%Y-%m-%d") + ".log"
        self.logger = Logger("DatabaseLogger", self.log_file_name).get_logger()
        self.variables = Variables("MYSQL")
        self.config = self.variables.get_variable()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            self.logger.info("Connected to MYSQL database.")
        except mysql.connector.Error as e:
            self.logger.error(f"Error connecting to MYSQL: {e}")


    def execute_query(self, query, params=None):
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connect()
            self.cursor.execute(query, params or ())
            self.connection.commit()
            self.logger.info(f"Query executed: {query}")
        except mysql.connector.Error as e:
            self.logger.error(f"Error executing query {e}")

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            self.logger.info("Disconnected from database")

    def fetch(self):
        try:
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching data: {e}")
            return None
