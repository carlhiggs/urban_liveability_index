# Purpose: Create tables in PostgreSQL database for MB-linked predicted N02 from csv file
#           -  First, take supplied xlsx file and reduce to two columns: 
#                        - MB_CODE11
#                        - PRED_NO2_2011_COL_PPB
#           - save this as a csv file, with relative location recorded in config.ini
# Author:  Carl Higgs
# Date:    28/03/2017

import os
import psycopg2
import time
import sys
from progressor import progressor
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task =  'Create tables in PostgreSQL database for MB-linked predicted N02 from csv file'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')

# SQL Settings
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
no2_table   = parser.get('air_pollution', 'no2_table')
  
no2_source = os.path.join(folderPath,parser.get('air_pollution', 'no2_source'))

createTable     = '''
  CREATE TABLE IF NOT EXISTS {} 
 (mb_code11 bigint PRIMARY KEY,
  pred_no2_2011_col_ppb double precision NOT NULL
 );'''.format(no2_table)
  
SQL_STATEMENT = """
    COPY {} FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """.format(no2_table)

def process_file(conn, table_name, file_object):
    cursor = conn.cursor()
    cursor.copy_expert(sql=SQL_STATEMENT, file=file_object)
    conn.commit()
    cursor.close()
    

# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
cur = conn.cursor()

# drop existing table, if it exists
cur.execute("DROP TABLE IF EXISTS %s;" % no2_table)
     
# (re-)create table
print(createTable+"... "),
cur.execute(createTable)    

# copy csv rows
my_file = open(no2_source)
process_file(conn, my_file, my_file)
print("Done.")

conn.close()
  
# output to completion log    
script_running_log(script, task, start)