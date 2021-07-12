# Purpose: Creates table with non-ABS area linkage codes for parcel
# Author:  Carl Higgs
# Date:    20/05/2017

# Import arcpy module

import arcpy
import numpy
import os
import time
import psycopg2 
from progressor import progressor
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Summarise non-ABS area linkage codes'

## INPUT SETTINGS
folderPath = parser.get('data', 'folderPath')

urbanGDB = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = urbanGDB  
arcpy.env.scratchWorkspace = folderPath   
arcpy.env.overwriteOutput = True 

srid = int(parser.get('workspace', 'srid'))

#linkage data locations
lga         = os.path.join(folderPath,folderPath,parser.get('abs', 'abs_lga'))
meshblock   = os.path.join(folderPath,folderPath,parser.get('abs', 'meshblocks'))
suburb      = os.path.join(folderPath,folderPath,parser.get('abs', 'abs_suburb'))

A_points = parser.get('parcels', 'parcel_dwellings')
A_points_ID = parser.get('parcels', 'parcel_id')

outfc = 'non_abs_linkage'
tempfc = os.path.join(arcpy.env.scratchGDB,'{}_temp'.format(outfc))
# alt_tempDir = os.path.join(folderPath,'scratch.gdb','{}_temp'.format(outfc))

fields = [A_points_ID,'SSC_CODE','SSC_NAME','LGA_CODE11','LGA_NAME11']
fieldt = ['varchar','integer','varchar','integer','varchar']

fieldl = [x.lower() for x in fields]

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName    = parser.get('postgresql', 'database')
sqlUserName  = parser.get('postgresql', 'user')
sqlPWD       = parser.get('postgresql', 'password')
sqlTableName = "non_abs_linkage"

createTable     = '''
  CREATE TABLE {}
  ({} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL ,
   {} {} NOT NULL 
  );
  '''.format(sqlTableName,fieldl[0],fieldt[0],
                          fieldl[1],fieldt[1],
                          fieldl[2],fieldt[2],
                          fieldl[3],fieldt[3],
                          fieldl[4],fieldt[4])

queryPartA      = '''
  INSERT INTO {} VALUES 
  '''.format(sqlTableName)
sqlChunkify     = 500


# OUTPUT PROCESS
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

# Temporary linkage of MetroUrban_ParcelDwellings to LGA
arcpy.MakeFeatureLayer_management(lga,"lga")
arcpy.SpatialJoin_analysis(target_features=A_points, 
                            join_features="lga", 
                            out_feature_class=tempfc, 
                            join_operation="JOIN_ONE_TO_ONE", 
                            join_type="KEEP_ALL", 
                            field_mapping='''
                            {0} "{0}" true true false 30 Text 0 0 ,First,#,{1},{0},-1,-1;
                            LGA_CODE11 "LGA_CODE11" true true false 5 Text 0 0 ,First,#,{2},LGA_CODE11,-1,-1;
                            LGA_NAME11 "LGA_NAME11" true true false 50 Text 0 0 ,First,#,{2},LGA_NAME11,-1,-1'''.format(A_points_ID, A_points, "lga"),  
                            match_option="INTERSECT", 
                            search_radius="", 
                            distance_field_name="")

# temp linkage of Address indexed LGA temp shape with SSC
arcpy.MakeFeatureLayer_management(suburb,"suburb")
arcpy.MakeFeatureLayer_management(tempfc,"tempfc")
arcpy.SpatialJoin_analysis(target_features="tempfc", 
                            join_features="suburb", 
                            out_feature_class=outfc, 
                            join_operation="JOIN_ONE_TO_ONE", 
                            join_type="KEEP_ALL", 
                            field_mapping='''
                            {0} "{0}" true true false 30 Text 0 0 ,First,#,{1},{0},-1,-1;
                            LGA_CODE11 "LGA_CODE11" true true false 5 Text 0 0 ,First,#,{1},LGA_CODE11,-1,-1;
                            LGA_NAME11 "LGA_NAME11" true true false 50 Text 0 0 ,First,#,{1},LGA_NAME11,-1,-1;
                            SSC_CODE "SSC_CODE" true true false 6 Text 0 0 ,First,#,{2},SSC_CODE,-1,-1;
                            SSC_NAME "SSC_NAME" true true false 45 Text 0 0 ,First,#,{2},SSC_NAME,-1,-1'''.format(A_points_ID,"tempfc", "suburb"), 
                            match_option="INTERSECT", 
                            search_radius="", 
                            distance_field_name="")
                            
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()

# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS %s;" % sqlTableName)
conn.commit()
curs.execute(createTable)
conn.commit()

# Expected denominator (metro urban parcel count) - for progress tracking
denom  = int(arcpy.GetCount_management(outfc).getOutput(0))
try:     
  startCount = time.time()  

  with arcpy.da.SearchCursor(outfc, fields) as cursor:
    count = 0
    chunkedLines = list()
    progressor(count,denom,startCount,sqlTableName)  
    for row in cursor:
      count += 1
      chunkedLines.append("($${}$$,{},$${}$$,{},$${}$$)".format(row[0],row[1],row[2],row[3],row[4]))
      if(count % sqlChunkify == 0):
        curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
        conn.commit()
        chunkedLines = list()
      progressor(count,denom,startCount,sqlTableName)
    if(count % sqlChunkify != 0):
      curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
      conn.commit()
  
finally:
  conn.close()      
  
  # output to completion log    
  script_running_log(script, task, start)