# Purpose: Extracts parcel PFI and meshblock code from feature
#          and export these to a postgreSQL database; also sets up meshblock linkage
# Author:  Carl Higgs
# Date:    13/12/2016

# Import arcpy module

import arcpy
import numpy
import os
import time
import psycopg2 
from shutil import copytree,rmtree
from progressor import progressor

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
urbanGDB    = os.path.join(folderPath,parser.get('data', 'workspace'))  
arcpy.env.workspace = urbanGDB
arcpy.env.overwriteOutput = True 

A_points = parser.get('parcels', 'parcel_dwellings')
# specify the unique location identifier 
pointsID = parser.get('parcels', 'parcel_id')

srid = int(parser.get('workspace', 'srid'))

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
parcel_mb_table    = "parcelmb"
abs_linkage_table    = "abs_linkage"

create_parcelmb_Table     = '''
  CREATE TABLE {} 
  ({} varchar(15) NOT NULL ,
   MB_CODE11 bigint NOT NULL,
   point_count integer NOT NULL);'''.format(parcel_mb_table,pointsID)
   
queryPartA1      = "INSERT INTO {} VALUES ".format(parcel_mb_table)
sqlChunkify     = 500


fields = ['MB_CODE11','SA1_7DIG11','SA2_NAME11','SA3_NAME11','STE_NAME11','dwellings','Shape@WKT']

# note - the list below intentionally doesn't include the geom type for shape@wkt, as we specify this manually renamed as 'geom'
fieldt = ['bigint','integer','varchar','varchar','varchar','integer']
fieldl = [x.lower() for x in fields]

create_abslinkage_Table     = '''
  CREATE TABLE {}
  ({} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {},
   geom geometry NOT NULL
  );
  '''.format(abs_linkage_table,fieldl[0],fieldt[0],
                          fieldl[1],fieldt[1],
						  fieldl[2],fieldt[2],
						  fieldl[3],fieldt[3],
						  fieldl[4],fieldt[4],
						  fieldl[5],fieldt[5])

queryPartA2      = '''
  INSERT INTO {} VALUES 
  '''.format(abs_linkage_table)


# OUTPUT PROCESS
task = 'Extract parcel PFI and meshblock code from {}, and create ABS linkage table.'.format(A_points)
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()

# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS {};".format(parcel_mb_table))
conn.commit()
curs.execute(create_parcelmb_Table)
conn.commit()


try:
  with arcpy.da.SearchCursor(A_points, [pointsID, 'MB_CODE11','COUNT_OBJECTID']) as cursor:
    count = 0
    chunkedLines = list()
    for row in cursor:
      count += 1
      chunkedLines.append("('{}',{},{})".format(row[0],row[1],row[2]))
      if(count % sqlChunkify == 0):
        curs.execute(queryPartA1 + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
        conn.commit()
        chunkedLines = list()
    if(count % sqlChunkify != 0):
      curs.execute(queryPartA1 + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
      conn.commit()
    print("Parcel-meshblock linkage table created.  Now creating abs_linkage table")
  
  curs.execute("DROP TABLE IF EXISTS {};".format(abs_linkage_table))
  conn.commit()  
  curs.execute(create_abslinkage_Table)
  conn.commit()
  
  startCount = time.time()  
  denom  = int(arcpy.GetCount_management("MB2011_DwellingPersons").getOutput(0))
  with arcpy.da.SearchCursor("MB2011_DwellingPersons", fields) as cursor:
    count = 0
    chunkedLines = list()
    progressor(count,denom,startCount,abs_linkage_table)
    
    for row in cursor:
      count += 1
      wkt = "ST_GeometryFromText('{}', {})".format(row[6].encode('utf-8').replace(' NAN','').replace(' M ',''),srid)
      chunkedLines.append("({},{},$${}$$,$${}$$,$${}$$,{},{})".format(row[0],row[1],row[2],row[3],row[4],row[5],wkt))
      if(count % sqlChunkify == 0):
        curs.execute(queryPartA2 + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
        conn.commit()
        chunkedLines = list()
      progressor(count,denom,startCount,abs_linkage_table)
    if(count % sqlChunkify != 0):
      curs.execute(queryPartA2 + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
      conn.commit()
  
      
finally:
  # output to completion log    
  script_running_log(script, task, start)
  
  # clean up
  conn.close()
   