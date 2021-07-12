# Purpose: This script recompiles the destinations geodatabase:
#             - converts multi-point to point where req'd
#             - clips to study region
#             - restricts to relevant destinations
#             - removes redundant columns
#             - compile as a single feature.
#             - A point ID is comma-delimited in form "Destionation,OID"
#               - this is to facilitate output to csv file following OD matrix calculation
#
#
# Author:  Carl Higgs
# Date:    13/03/2017

import os
import pandas
import arcpy
import time
import sys
import numpy
import json
import psycopg2
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

reload(sys)
sys.setdefaultencoding('utf8')

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')


# Source destination geodatabase
dest_gdb = os.path.join(folderPath,parser.get('destinations', 'src_destinations'))  
proj_gdb = os.path.join(folderPath,parser.get('data', 'workspace'))  


# Output restricted geodatabase (to be created)
melbHexGdb = os.path.join(folderPath,parser.get('destinations', 'study_destinations'))  

# polygon feature to clip destinations to (based on instersect)
clippingFeature = os.path.join(proj_gdb,parser.get('workspace', 'hex_grid_buffer'))

# a feature to which destination features will also be appended
#   (this is in addition to seperate created features)
outCombinedFeature = os.path.splitext(os.path.basename(melbHexGdb))[0]


# List of potentially relevant destinations, defined in config file
destination_list = parser.get('destinations', 'destination_list').split(',')

def int_else_string(s):
    try:
        return int(s)
    except ValueError:
        return s

destination_cutoff = parser.get('destinations','destination_cutoff')
destination_cutoff = [int_else_string(x) for x in destination_cutoff.split(',')]

# define spatial reference
spatial_reference = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))

# auto-setup workspace
arcpy.env.workspace = dest_gdb
arcpy.env.scratchWorkspace = folderPath  
scratchOutput = os.path.join(arcpy.env.scratchGDB,'MultiPointToPointDest')
arcpy.env.overwriteOutput = True 


# SQL Settings 
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

sqlTableName = "dest_type"
queryPartA = "INSERT INTO {} VALUES ".format(sqlTableName)

sqlChunkify = 50

createTable = '''
  CREATE TABLE %s
  (dest integer PRIMARY KEY,
   dest_name varchar NOT NULL,
   dest_count integer,
   dest_cutoff integer);
   ''' % sqlTableName



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
# Compile restricted gdb of destination features
task = 'Recompile destinations from {} to more concise geodatabase {}'.format(dest_gdb,melbHexGdb)
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))

if arcpy.Exists(melbHexGdb) is False:  
  arcpy.CreateFileGDB_management(folderPath,parser.get('destinations', 'study_destinations'))
  print("created "+melbHexGdb)

# compile a list of datasets to be checked over for valid features within the destination GDB
datasets = arcpy.ListDatasets(feature_type='feature')
 
dest_count = numpy.empty(len(destination_list), dtype=int)
 
for ds in datasets:
  for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
    if fc in destination_list:
      destNum = destination_list.index(fc)
      # Make sure all destinations conform to shape type 'Point' (ie. not multipoint)
      if arcpy.Describe(fc).shapeType != u'Point':
        arcpy.FeatureToPoint_management(fc, scratchOutput, "INSIDE")
        arcpy.MakeFeatureLayer_management(scratchOutput,'destination')  
      else:
        # Select and copy destinations intersecting Melbourne hexes
        arcpy.MakeFeatureLayer_management(fc,'destination')                                            

      selection = arcpy.SelectLayerByLocation_management('destination', 'intersect',clippingFeature)
      count = int(arcpy.GetCount_management(selection).getOutput(0))
      dest_count[destNum] = count
      # export selected rows as CSV with full original data; can be link to reduced set or original dest gdb using OID
      # 'Conditions_on_Approval' is suspected problematic field in childcare_outofhours, which 
      # may cause memory error due to excessive length - > 2billion chars?!?!?!
	  # 'snippet' in universities is also millions of chars long --- too long, and redundant.
      field_names = [f.name.encode('utf-8') for f in arcpy.ListFields(fc) if f.type != u'Date' and f.name != 'Conditions_on_Approval' and f.name != 'Snippet']
      destData = arcpy.da.FeatureClassToNumPyArray(selection, field_names) 
      field_names[1] =  spatial_reference.name.encode('utf-8')
      field_names = ','.join(field_names)      
      
      fcNum  = 'dest{}-{}'.format(destNum,fc)
      
      numpy.savetxt(os.path.join(melbHexGdb,'{}.csv').format(fcNum), destData, delimiter=",", fmt="%s", header=field_names)
      # mlab.rec2csv(destData,os.path.join(melbHexGdb,fc+'.csv'), delimiter=',', formatd=None, missing='', missingd=None, withheader=True)
      
      # Remove all fields from destination, and repurpose existing ObjectID field for later reshaping
      renameSkinny(is_geo = True, 
                   in_obj = selection, 
                   out_obj = 'featureTrimmed')
                   
      arcpy.CopyFeatures_management('featureTrimmed', os.path.join(melbHexGdb,fc))
      
      # Note that the field length of this object may need to be increased (or could be decreased) depending on numer / original OIDs of selected destinations.  A length of 8 characters allows for selected OIDs in the tens of thousands with a two digit destination identifier.
      arcpy.AddField_management(os.path.join(melbHexGdb,fc), 'Dest_OID', 'TEXT', field_length=8)
                          
      # Fill in rows, including linkage object ID
      i = 0 
      with arcpy.da.UpdateCursor(os.path.join(melbHexGdb,fc), 'Dest_OID') as cursor:
        for row in cursor:        
          row[0] = '{:02},{}'.format(destNum,destData['OBJECTID'][i])
          cursor.updateRow(row)
          i += 1
      
      if arcpy.Exists(os.path.join(melbHexGdb,outCombinedFeature)) is False:  
        arcpy.CreateFeatureclass_management(out_path = melbHexGdb, 
                                            out_name = outCombinedFeature,
                                            template = os.path.join(melbHexGdb,fc))
      arcpy.MakeFeatureLayer_management(os.path.join(melbHexGdb,fc),"destToAppend")                                            
      arcpy.Append_management(os.path.join(melbHexGdb,fc), os.path.join(melbHexGdb,outCombinedFeature))
      print("Processed: {} ({} points)".format(fc,count))
      
# Define projection as GDA 1994 VicGrid94
arcpy.DefineProjection_management(os.path.join(melbHexGdb,outCombinedFeature), spatial_reference)


# # OUTPUT PROCESS
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
  
# drop table if it already exists
curs.execute("DROP TABLE IF EXISTS %s;" % sqlTableName)
conn.commit()
curs.execute(createTable)
conn.commit()

for i in range(0,len(destination_list)):
  curs.execute(queryPartA + "({},'{}',{},{})".format(i,destination_list[i],dest_count[i],destination_cutoff[i]) +' ON CONFLICT DO NOTHING')
  conn.commit()

print("Created 'dest_type' destination summary table for database {}.".format(sqlDBName))
conn.close()
  
  
# output to completion log    
script_running_log(script, task, start)


