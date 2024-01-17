import os
import csv
from config.constants import *


def fileWriter(data, scriptPrefix):
    fileName = f'{scriptPrefix}{CSV_EXTENSION}'
    tmpPath = os.path.join(os.getenv('tmp_path'), fileName)

    with open(tmpPath, 'w', newline = '') as csvFile:
        csvWriter = csv.writer(csvFile)
        csvWriter.writerows(data)

def truncateTable(db, schema, table):
    return f"TRUNCATE TABLE {db}.{schema}.{table}"


    
