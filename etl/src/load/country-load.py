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
    tmpQuery = f'''
        INSERT INTO {TARGET_DB}.{TMP_SCHEMA}.COUNTRY
        SELECT 
            ID,
            COUNTRY_DESC
        FROM {TARGET_DB}.{LND_SCHEMA}.{scriptPrefix.upper()}
    '''
    snow.executeQuery(tmpQuery)

    mergeQuery = f'''
        MERGE INTO {TARGET_DB}.{TGT_SCHEMA}.{scriptPrefix.upper()} AS TGT
        USING {TARGET_DB}.{TMP_SCHEMA}.{scriptPrefix.upper()} AS TMP
        ON TGT.ID = TMP.ID
        WHEN MATCHED AND TGT.ACTIVE_STATUS = True AND TGT.COUNTRY_DESC <> TMP.COUNTRY_DESC
        THEN 
            UPDATE
            SET 
                TGT.ACTIVE_STATUS = False,
                TGT.EFFECTIVE_END_DT = CURRENT_DATE - 1,
                TGT.REC_UPD_TS = CURRENT_TIMESTAMP
        WHEN MATCHED AND TGT.ACTIVE_STATUS = True AND TGT.COUNTRY_DESC = TMP.COUNTRY_DESC
        THEN
            UPDATE
            SET
                TGT.REC_UPD_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED
        THEN 
        INSERT (ID, COUNTRY_KEY, COUNTRY_DESC, EFFECTIVE_START_DT, EFFECTIVE_END_DT, ACTIVE_STATUS, REC_INS_TS, REC_UPD_TS)
        VALUES
            (
                TMP.ID, 
                (SELECT NVL(MAX(COUNTRY_KEY), 0) FROM {TARGET_DB}.{TGT_SCHEMA}.{scriptPrefix.upper()}TGT) + RANK() OVER(ORDER BY ID), 
                TMP.COUNTRY_DESC,
                CURRENT_DATE,
                '9999-12-30',
                True,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            );
    '''

    snow.executeQuery(mergeQuery)
    scdQuery = f'''
        INSERT INTO {TARGET_DB}.{TGT_SCHEMA}.{scriptPrefix.upper()}
        SELECT 
            TMP.ID, 
            (SELECT NVL(MAX(COUNTRY_KEY), 0) FROM {TARGET_DB}.{TGT_SCHEMA}.COUNTRY) + RANK() OVER(ORDER BY ID), 
            TMP.COUNTRY_DESC,
            CURRENT_DATE,
            '9999-12-30',
            True,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM {TARGET_DB}.{TMP_SCHEMA}.{scriptPrefix.upper()}TMP
        WHERE EXISTS 
        (SELECT 1 FROM {TARGET_DB}.{TGT_SCHEMA}.{scriptPrefix.upper()}TGT
            WHERE 
                TMP.ID = TGT.ID
            AND TMP.COUNTRY_DESC <> TGT.COUNTRY_DESC
        );
    '''
    snow.executeQuery(scdQuery)
    logger.info(f"Loading for {scriptPrefix.upper()} completed!")
    
except Exception as e:
    logger.error(e)
    raise Exception(f"[ERROR]: {e}")
finally:
    snow.disconnect()