import os
from utilities.utils import *
from libs.Logger import Logger
from config.constants import *
from libs.Snowflake import Snowflake

scriptName = os.path.basename(__file__)
scriptPrefix = scriptName.split('-')[0]

#Object Initialization
logger = Logger(scriptPrefix)
snow = Snowflake(logger, scriptPrefix)
logger.info(f"Started executing the script: {scriptName}")

try:
    rows = snow.sourceExtractor(SOURCE_DB, SRC_SCHEMA)
    fileWriter(rows, scriptPrefix)
    snow.stageBuilder()

    logger.info(f"Truncating Table {scriptPrefix}")
    query = truncateTable(TARGET_DB, LND_SCHEMA, scriptPrefix)
    snow.executeQuery(query)

    snow.copyIntoLanding()
        
except Exception as e:
    logger.error(e)
    raise Exception(f"[ERROR]: {e}")
finally:
    snow.disconnect()