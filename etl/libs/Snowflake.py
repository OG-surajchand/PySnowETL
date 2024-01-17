import os
import snowflake.connector
from dotenv import load_dotenv
from libs.Logger import Logger
from config.constants import *

load_dotenv()

class Snowflake:
    def __init__(self, logger: Logger, scriptPrefix):
        self.parameters = {
            'user': os.getenv('user'),
            'password': os.getenv('password'),
            'account': os.getenv('account'),
            'warehouse': os.getenv('warehouse'),
            'database': os.getenv('database')
        }
        self.table = scriptPrefix.upper()
        self.logger = logger
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        try:
            connection = snowflake.connector.connect(**self.parameters)
            return connection
        except Exception as e:
            self.logger.error(f"Error connecting to Snowflake: {str(e)}")
            raise

    def executeQuery(self, queryString):
        self.logger.info(queryString)
        try:
            self.cursor.execute(queryString)
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise

    def fetchOne(self, queryString):
        self.logger.info(queryString)
        try:
            result = self.cursor.execute(queryString).fetchone()
            return result
        except Exception as e:
            self.logger.error(f"Error fetching one result: {str(e)}")
            raise

    def fetchAll(self, queryString):
        self.logger.info(queryString)
        try:
            result = self.cursor.execute(queryString).fetchall()
            return result
        except Exception as e:
            self.logger.error(f"Error fetching all results: {str(e)}")
            raise
    
    def stageBuilder(self):
        self.logger.info("Started PUT operation of files into Snowflake Stage.")
        fileName = f'{self.table}{CSV_EXTENSION}'
        tmpPath = os.getenv('tmp_path')

        self.executeQuery(f"USE DATABASE {TARGET_DB};")
        self.executeQuery(f"USE SCHEMA {STG_SCHEMA};")
        self.executeQuery(f"CREATE OR REPLACE FILE FORMAT {CSV_FILE_FORMAT} TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = 'AUTO';")
        self.executeQuery(f"CREATE OR REPLACE STAGE {INTERNAL_STAGE}_{self.table} file_format = {CSV_FILE_FORMAT};")
        self.executeQuery(f"PUT 'file://{tmpPath}/{fileName}' @{INTERNAL_STAGE}_{self.table};")
        
    
    def sourceExtractor(self, database, schema):
        self.logger.info(f"Downloading the data for the source: {self.table}.")

        queryString = f'''
            SELECT * FROM {database}.{schema}.{self.table};
        '''
        result = self.fetchAll(queryString)
        return result
    
    def copyIntoLanding(self):
        queryString = f'''
            COPY INTO {TARGET_DB}.{LND_SCHEMA}.{self.table}
            FROM @{TARGET_DB}.{STG_SCHEMA}.{INTERNAL_STAGE}_{self.table}
            FILE_FORMAT = (FORMAT_NAME = {CSV_FILE_FORMAT})
        '''
        self.executeQuery(queryString)

    def disconnect(self):
        try:
            self.connection.close()
        except Exception as e:
            self.logger.error(f"Error disconnecting from Snowflake: {str(e)}")
            raise
