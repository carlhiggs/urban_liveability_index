# Purpose: Clip addresses to Vic Urban Metro area (if Dwellings present) and associate w/ hex
# Author:  Carl Higgs
# Date:    2016 11 01


import os
import pandas
import arcpy
import time
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Project meshblock shapefile to correct spatial reference; join w/ dwelling data;  crop to Metro Urban area; and associate w/ hex'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
destGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  
arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

spatial_reference = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))

address = os.path.join(folderPath,parser.get('parcels', 'parcels'))
address_id = parser.get('parcels', 'parcel_id')

inclusion_region =  parser.get('data','inclusion_region')

meshblock_dwellings =  parser.get('workspace','mb_dwellings')
meshblock_id = parser.get('abs', 'meshblock_id')

## Hex details (polygon feature to iterate over)
hex_grid = parser.get('workspace', 'hex_grid')


scratch_shape  = os.path.join(arcpy.env.scratchGDB,'scratch_shape')

# parcel meshblock dwellings output -- temporary, and long term
scratch_points = os.path.join(arcpy.env.scratchGDB,'scratch_points')
scratch_doppel = os.path.join(arcpy.env.scratchGDB,'scratch_points_D')
parcel_dwellings = parser.get('parcels','parcel_dwellings')

# Define make reduced feature layer method
def renameSkinny(is_geo, in_obj, out_obj, keep_fields_list=[''], rename_fields_list=None, where_clause=''):
          ''' Make an ArcGIS Feature Layer or Table View, containing only the fields
              specified in keep_fields_list, using an optional SQL query. Default
              will create a layer/view with NO fields. Method amended (Carl 17 Nov 2016) to include a rename clause - all fields supplied in rename must correspond to names in keep_fields'''
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

print("Project to coordinate system {}... ".format(parser.get('workspace', 'SpatialRef')))
arcpy.MakeFeatureLayer_management(address, 'feature') 
arcpy.Project_management('feature', scratch_shape, spatial_reference)
arcpy.Delete_management('feature')
arcpy.MakeFeatureLayer_management(scratch_shape, 'feature')


print("Done.\nSelect parcels within the inclusion region ({})... ".format(inclusion_region))
selection = arcpy.SelectLayerByLocation_management(in_layer='feature', 
                                       overlap_type='intersect',
									   select_features=inclusion_region)
									   
print("Done.\nJoin (ie. restrict) study inclusion region-defined parcel address points to meshblocks with dwellings... ")
								   
arcpy.SpatialJoin_analysis(target_features   = selection, 
                           join_features     = meshblock_dwellings, 
                           out_feature_class = scratch_points, 
                           join_operation="JOIN_ONE_TO_ONE", 
                           join_type="KEEP_COMMON", 
                           field_mapping="""{0} "{0}" true true false 15 Text 0 0 ,First,#,{1},{0},-1,-1;{2} "{2}" true true false 11 Text 0 0 ,First,#,{3},{2},-1,-1""".format(address_id,selection,meshblock_id,meshblock_dwellings),
                           match_option="INTERSECT")
		
print("Done.\nDissolve on XY coordinates, including count of collapsed doppels... ")
# This can potentially remove a large number of redundant points, where they exist overlapping one another, and so have otherwise identical environmental exposure measurements.  ie. this data is redundant; instead, a field is added with a point count where overlaps were identified. 
arcpy.AddXY_management(scratch_points)

arcpy.Dissolve_management(scratch_points, 
                          scratch_doppel, 
						  dissolve_field="POINT_X;POINT_Y", 
						  statistics_fields="{} FIRST;{} FIRST;OBJECTID COUNT".format(address_id,meshblock_id), 
						  multi_part="SINGLE_PART")

print("Done.\nSpatially associate each parcel w/ a hex ... ")
arcpy.Delete_management(scratch_points)								   

arcpy.SpatialJoin_analysis(target_features   = scratch_doppel, 
                           join_features     = hex_grid, 
                           out_feature_class = scratch_points, 
                           join_operation="JOIN_ONE_TO_ONE", 
                           join_type="KEEP_ALL", 
                           field_mapping= """{0} "{0}" true true false 15 Text 0 0 ,First,#,{1},{2},-1,-1; {3} "{3}" true true false 11 Text 0 0 ,First,#,{1},{4},-1,-1;{5} "{5}" true true false 4 Long 0 0 ,First,#,{6},{5},-1,-1;{7} "{7}" true true false 4 Long 0 0 ,First,#,{1},{7},-1,-1;{8} "{8}" true true false 8 Double 0 0 ,First,#,{1},{8},-1,-1;{9} "{9}" true true false 8 Double 0 0 ,First,#,{1},{9},-1,-1""".format(address_id,scratch_doppel,'FIRST_'+address_id,meshblock_id,'FIRST_'+meshblock_id,'Input_FID',hex_grid,'COUNT_OBJECTID','POINT_X','POINT_Y'), 
                           match_option="INTERSECT")     

print("Done.\nAssociate parcel with overlaying hex (as the join provides input_fid, but not OBJECTID which is used as hex identifier... ")
                           
arcpy.AlterField_management (hex_grid, "OBJECTID",new_field_alias="HEX_ID")                        
arcpy.MakeFeatureLayer_management(scratch_points, 'points')
arcpy.MakeFeatureLayer_management(hex_grid, 'hex_grid')
                           
arcpy.AddJoin_management(in_layer_or_view = 'points', 
                         in_field         = 'Input_FID',
                         join_table       = 'hex_grid',
                         join_field       = 'Input_FID',
                         join_type        = "KEEP_ALL")     

print("Done.\nRename ID fields to original identifiers and export meshblock parcel dwellings feature to geodatabase... ")

oldfields = ['scratch_points.OBJECTID', 'scratch_points.Shape',  'scratch_points.{}'.format(address_id), 'scratch_points.{}'.format(meshblock_id), 'scratch_points.COUNT_OBJECTID', 'scratch_points.POINT_X', 'scratch_points.POINT_Y', '{}.OBJECTID'.format(hex_grid)] 
newfields = ['OBJECTID','Shape','{}'.format(address_id),'{}'.format(meshblock_id),'COUNT_OBJECTID','POINT_X','POINT_Y','HEX_ID']				 
renameSkinny(is_geo = True, 
             in_obj = 'points', 
             out_obj = 'tempFull', 
             keep_fields_list = oldfields, 
             rename_fields_list = newfields,
             where_clause = '')
print("Done.")	 

arcpy.CopyFeatures_management('tempFull', parcel_dwellings)    



# output to completion log					
script_running_log(script, task, start)

