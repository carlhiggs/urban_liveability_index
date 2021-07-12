# Purpose: Create tables in PostgreSQL database from csv files in specific folders
# Author:  Carl Higgs
# Date:    21/12/2016

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
task = 'Create tables in PostgreSQL database from csv files derived from ABS data'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')

# SQL Settings
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

  
singleTables = {
  os.path.join(folderPath,parser.get('abs', 'Adult18up_Employment')):'Adult18up_Employment',
  os.path.join(folderPath,parser.get('abs', 'CarOwnership')):'CarOwnership',
  os.path.join(folderPath,parser.get('abs', 'MethodOfTravelToWorkPlace')):'MethodOfTravelToWorkPlace',
  os.path.join(folderPath,parser.get('abs', 'LiveWorkSameSA3')):'LiveWorkSameSA3',
  os.path.join(folderPath,parser.get('abs', 'AffordableHousing')):'AffordableHousing',
  os.path.join(folderPath,parser.get('abs', 'OwnerOccupiedTORentalHousing')):'OwnerOccupiedTORentalHousing'
}  

singleFields = {
  'Adult18up_Employment':'SA1_7DIG11 integer PRIMARY KEY, employed integer NOT NULL, unemployed	integer NOT NULL, not_in_the_labour_force integer NOT NULL,	not_stated  integer NOT NULL,	total integer NOT NULL',
  'CarOwnership':'SA1_7DIG11 integer PRIMARY KEY,no_mv integer NOT NULL,one_mv integer NOT NULL, two_mv integer NOT NULL,three_mv integer NOT NULL, fourUp_mv integer NOT NULL, not_stated_mv integer NOT NULL,	na_mv integer NOT NULL, total_mv integer NOT NULL',
  'MethodOfTravelToWorkPlace':'SA1_7DIG11 integer PRIMARY KEY, mtwp_private integer NOT NULL, mtwp_active integer NOT NULL, mtwp_public integer NOT NULL, mtwp_total integer NOT NULL',
  'LiveWorkSameSA3':'SA2_NAME11 varchar PRIMARY KEY, SA3_live_and_work integer NOT NULL,SA3_work integer NOT NULL,SA3_prop_live_work  double precision',
  'AffordableHousing':'SA1_7DIG11 integer PRIMARY KEY, hous_cost_le30pct_hhinc integer NOT NULL,	hous_cost_gr30pct_hhinc integer NOT NULL, validtot_1st2nd_hhinc_quint integer NOT NULL',
  'OwnerOccupiedTORentalHousing':'SA1_7DIG11 integer PRIMARY KEY,owner_occupied	 integer NOT NULL,rental	 integer NOT NULL'
} 

singleDenom = len(singleTables) 
 
filter = ['log','summary']

  
SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

def process_file(conn, table_name, file_object):
    cursor = conn.cursor()
    cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=file_object)
    conn.commit()
    cursor.close()
    

# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
cur = conn.cursor()

print("Processing:")


try:        
  count = 0  
  denom = len(singleTables)
  startCount = time.time()
  for dir, table in singleTables.items():
    count += 1
    progressor(count,denom,startCount, '{}'.format(singleTables[dir]))
    # drop existing table, if it exists
    cur.execute("DROP TABLE IF EXISTS %s;" % singleTables[dir])
         
    # (re-)create table
    createTable     = 'CREATE TABLE IF NOT EXISTS {} ({});'.format(singleTables[dir],singleFields[singleTables[dir]])
    print(createTable+"... "),
    cur.execute(createTable)    
    
    # copy csv rows
    my_file = open(dir)
    process_file(conn, singleTables[dir], my_file)
    print("Done.")
  
except (Exception, psycopg2.DatabaseError) as error:
  print(error) 
    
finally:
  conn.close()
  
# output to completion log    
script_running_log(script, task, start)