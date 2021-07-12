# Purpose: Simplify POS entry points and polygons
#   - create minimal attribute ds w/ joint [poly,point]
#   - create SQL attribute summary table
#       - index code (veac_id) 
#       - OS category (os_group) 
#       - size in hectares (ha)
#
#   NOTE: the VEAC pos source as currently used (with 50m vertices)
#         seems to be for metro melb only.... So this script has not been run for Ballarat
# Author:  Carl Higgs
# Date:    2017 03 19

import arcpy
import os
import time
import numpy as np
import sys
import psycopg2
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Simplify POS entry points and polygons and export table /w polyID and area'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
destGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  
arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

POSentry = parser.get('pos', 'pos_entry')

polyID            = parser.get('pos', 'pos_poly_id')
polyID_maxLength  = parser.get('pos', 'poly_id_maxLength')
pointID           = parser.get('pos', 'pos_point_id')
pointID_maxLength = parser.get('pos', 'point_id_maxLength')

# Comma concatenated linkID to be created from joint of poly and point IDs
linkID = 'POS_entryID'
linkID_stringLength = int(polyID_maxLength)+int(pointID_maxLength)+1

# POS categories
category = parser.get('pos', 'pos_categories')

# linkage csv file field names
field_names = [polyID,category,'Area_sqm']

## the below might be appropriate if using polygon data, however we have point data with a field for area
# area = "SHAPE@AREA"
area = "HA"

# output name for trimmed POS entry with ID only
output = POSentry+'_POSentryIDonly'

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlTableName = "pos_attribute"
queryPartA = "INSERT INTO {} VALUES ".format(sqlTableName)
sqlChunkify = 50

# initiate postgresql connection
conn = psycopg2.connect(database=parser.get('postgresql', 'database'), 
                        user=parser.get('postgresql', 'user'),
                        password=parser.get('postgresql', 'password'))
curs = conn.cursor()

createTable = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE {0}
  (veac_id varchar PRIMARY KEY,
   os_group varchar NOT NULL,
   area_ha double precision NOT NULL);
   '''.format(sqlTableName)


# Define make reduced feature layer method
def renameSkinny(is_geo, in_obj, out_obj, keep_fields_list=[''], rename_fields_list=None, where_clause=''):
          ''' Make an ArcGIS Feature Layer or Table View, containing only the fields
              specified in keep_fields_list, using an optional SQL query.
              Can include a rename clause - all fields supplied in rename must 
              correspond to names in keep_fields'''
          field_info_str = ''
          input_fields = arcpy.ListFields(in_obj)
          if not keep_fields_list:
              keep_fields_list = []
          i = 0
          for field in input_fields:
              if field.name in keep_fields_list:
                  possibleNewName = (rename_fields_list[i],field.name)[rename_fields_list==None]
                  field_info_str += field.name + ' ' + possibleNewName + ' VISIBLE;'
                  i += 1
              else:
                  field_info_str += field.name + ' ' + field.name + ' HIDDEN;'
          field_info_str.rstrip(';')  # Remove trailing semicolon
          if is_geo:
              arcpy.MakeFeatureLayer_management(in_obj, out_obj, where_clause, field_info=field_info_str)
          else:
              arcpy.MakeTableView_management(in_obj, out_obj, where_clause, field_info=field_info_str)
          return out_obj


# OUTPUT PROCESS      
# Fill in rows, including linkage object ID, and field for shape area in metres (as integer)
arcpy.AddField_management(POSentry,linkID, 'TEXT', field_length=linkID_stringLength)
# arcpy.AddField_management (POSentry, "Area_sqm", "LONG")

# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS %s;" % sqlTableName)
conn.commit()
curs.execute(createTable)
conn.commit()

with arcpy.da.UpdateCursor(POSentry, [polyID,pointID,linkID,category,area]) as cursor:
  count = 0
  chunkedLines = list()  
  for row in cursor:
    count += 1
    # update feature  
    row[2] = '{},{}'.format(row[0],str(row[1]))
    cursor.updateRow(row)
    
    # accumulate attribute data for SQL table
    chunkedLines.append("('{}','{}',{})".format(row[0],row[3],row[4]) ) 
    if (count % sqlChunkify == 0) :
      curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
      conn.commit()
      chunkedLines = list() 
      
  if(count % sqlChunkify != 0):
    curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
    conn.commit()    

renameSkinny(is_geo = True,in_obj = POSentry,out_obj = 'featureTrimmed',keep_fields_list=linkID,rename_fields_list=linkID)             
             
arcpy.CopyFeatures_management('featureTrimmed', output)
 
# output to completion log    
script_running_log(script, task, start)
conn.close()