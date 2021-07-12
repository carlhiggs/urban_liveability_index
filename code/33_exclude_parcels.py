# Purpose: This script develops a list of suspect parcels to investigate and exclude, based on having
#          null values for key indicators.  This usually arises for study region edge cases with poor
#          connectivity to network; implication is that these are not adequate representations of 
#          residential parcels (as we cannot reliably link to road network), so are best excluded.
# Author:  Carl Higgs

import arcpy
import os
import sys
import time
import psycopg2
import numpy as np
from progressor import progressor

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = "Create list of exlcuded parcels, based on null values for indicators"


# INPUT PARAMETERS

## specify locations
points =  parser.get('parcels','parcel_dwellings')
pointsID = parser.get('parcels', 'parcel_id')

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

# output tables
# In this table detail_pid is not unique --- the idea is that jointly with indicator, detail_pid will be unique; such that we can see which if any parcels are missing multiple indicator values, and we can use this list to determine how many null values each indicator contains (ie. the number of detail_pids for that indicator)
# The number of excluded parcels can be determined through selection of COUNT(DISTINCT(detail_pid))
createTable_exclusions     = '''
  DROP TABLE IF EXISTS excluded_parcels;
  CREATE TABLE excluded_parcels
  ({0} varchar NOT NULL,
    indicator varchar NOT NULL,  
  PRIMARY KEY({0},indicator));
  '''.format(pointsID.lower())

qA = "INSERT INTO excluded_parcels SELECT a.detail_pid, '"
qB = "\nFROM parcelmb AS a LEFT JOIN "
qC = " AS b \n ON a.detail_pid = b.detail_pid \n WHERE "
qD = " IS NULL ON CONFLICT (detail_pid,indicator) DO NOTHING "
  
# exclude on null indicator, and on null distance
query = '''
{0} walkability'                  {1} ind_walkability_hard  {2} walkability                    {3};
{0} si_mix'                       {1} ind_si_mix_hard       {2} si_mix                         {3};
{0} dest_pt'                      {1} ind_dest_pt_hard      {2} dest_pt                        {3};
{0} walkability'                  {1} ind_walkability_soft  {2} walkability                    {3};
{0} si_mix'                       {1} ind_si_mix_soft       {2} si_mix                         {3};
{0} dest_pt'                      {1} ind_dest_pt_soft      {2} dest_pt                        {3};
{0} pos_greq15000m2_in_400m_soft' {1} ind_pos               {2} pos_greq15000m2_in_400m_soft   {3};
{0} dest_distance'                {1} dest_distance         {2} NOT (b IS NOT NULL);
{0} sa1_7dig11'                  {1} abs_linkage ON a.mb_code11 = abs_linkage.mb_code11 
    WHERE abs_linkage.sa1_7dig11 NOT IN (SELECT sa1_7dig11 FROM abs_2011_irsd)
    ON CONFLICT (detail_pid,indicator) DO NOTHING;
'''.format(qA,qB,qC,qD)

# OUTPUT PROCESS

conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()

curs.execute(createTable_exclusions)
conn.commit()

curs.execute(query)
conn.commit()

# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()
 
 
 
 
 