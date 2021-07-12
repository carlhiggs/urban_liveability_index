# Purpose: create ABS irsd table
# Author:  Carl Higgs 
# Date:    20170216

import os,sys
import time
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create ABS irsd table'

A_pointsID = parser.get('parcels', 'parcel_id')

irsd = os.path.join(parser.get('data','folderPath'),parser.get('abs', 'abs_irsd'))

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

createTable = '''
DROP TABLE IF EXISTS abs_2011_irsd;
CREATE TABLE abs_2011_irsd 
(SA1_7dig11	integer, usual_resident_pop	integer, irsd_score integer, aust_rank integer, 
 aust_decile integer, aust_pctile integer, state varchar, state_rank integer, state_decile integer, state_pctile integer);
'''
   
# NOTE: the copy statement is commented out as it does not work from script context; run this as an interactive query  
   
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
curs = conn.cursor()
curs.execute(createTable)
curs.copy_expert(sql="COPY abs_2011_irsd FROM STDIN WITH CSV HEADER DELIMITER AS ',';", file=open(irsd))
conn.close()

# output to completion log    
script_running_log(script, task, start)



  