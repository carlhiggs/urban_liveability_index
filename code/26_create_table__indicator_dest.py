# Purpose: This script creates binary indicators based on pre-defined cutoffs as a table of indicators
#          It is to be used with a Postgresql database (e.g. for a liveability index) 
# Author:  Carl Higgs 
# Date:    17/1/2017

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'creates binary indicators based on pre-defined cutoffs as a table of indicators'

A_pointsID = parser.get('parcels', 'parcel_id')


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
dest_hard_table = "ind_dest_hard"
dest_soft_table = "ind_dest_soft"
queryPartA = "INSERT INTO {} VALUES ".format(dest_hard_table)

sqlChunkify = 50

# OUTPUT PROCESS
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
 
# get destination indicator names 
destinations = []
for i in range(0,30):
  curs.execute("SELECT lower(regexp_replace((SELECT dest_name||'_'||dest_cutoff||'m'FROM dest_type WHERE dest = {}), '[^a-zA-Z0-9\_]', '', 'g'))".format(i))
  # destinations.append([", " + dest[0]+ " smallint" for dest in curs])
  destinations.append([dest[0] for dest in curs])

# flatten nested list
destinations = [item for sublist in destinations for item in sublist]

# get cutoff distances   
cutoffs = []
curs.execute("SELECT dest_cutoff FROM dest_type WHERE dest < 30".format(i))
cutoffs = [item[0] for item in curs]


# Discrete destination table (binary indicators)
joinString = ''

for i in range(0,30):
  joinString = joinString + '''
  LEFT JOIN (SELECT dist_cl_od_parcel_dest.{0}, distance, (CASE 
  WHEN distance < {1}  THEN 1
  WHEN distance >= {1} THEN 0
  ELSE NULL END) as "{2}"
  FROM dist_cl_od_parcel_dest
  WHERE dest = {3}) AS ind{3} on parcelmb.{0} =ind{3}.{0} 
  '''.format(A_pointsID.lower(),cutoffs[i],destinations[i],i)

create_hard_dest_table = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE {0}
  AS SELECT parcelmb.{1} {2} FROM parcelmb {3} ;
  '''.format(dest_hard_table,A_pointsID.lower(),' '.join([", " + dest for dest in destinations]), joinString)

curs.execute(create_hard_dest_table)
conn.commit()
  
  
# Continuous destination table (aka 'soft cutoffs')
joinString = ''

for i in range(0,30):
  joinString = joinString + '''
  LEFT JOIN (SELECT dist_cl_od_parcel_dest.{0}, distance, 
  1-1/(1+exp(-5*(distance-{1})/{1}::double precision)) AS "{2}"
  FROM dist_cl_od_parcel_dest
  WHERE dest = {3}) AS ind{3} on parcelmb.{0} =ind{3}.{0} 
  '''.format(A_pointsID.lower(),cutoffs[i],destinations[i],i)

create_soft_dest_table = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE {0}
  AS SELECT parcelmb.{1} {2} FROM parcelmb {3} ;
  '''.format(dest_soft_table,A_pointsID.lower(),' '.join([", " + dest for dest in destinations]), joinString)

curs.execute(create_soft_dest_table)
conn.commit()


# create table of distance to destinations
joinString = ''

for i in range(0,30):
  joinString = joinString + '''
  LEFT JOIN (SELECT dist_cl_od_parcel_dest.{0}, distance AS {1}
  FROM dist_cl_od_parcel_dest
  WHERE dest = {2}) AS ind{3} on parcelmb.{0} =ind{4}.{0} 
  '''.format(A_pointsID.lower(),destinations[i],i,i,i)
		  
createTable = '''
  DROP TABLE IF EXISTS dest_distance;
  CREATE TABLE dest_distance
  AS SELECT parcelmb.{} {} FROM parcelmb {} ;
  '''.format(A_pointsID.lower(),' '.join([", " + dest for dest in destinations]), joinString)

curs.execute(createTable)
conn.commit() 
  
conn.close()
  
# output to completion log    
script_running_log(script, task, start)