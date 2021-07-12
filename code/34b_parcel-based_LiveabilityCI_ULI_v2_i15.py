# Purpose: create parcel-based liveability composite indicator
#          In particular, 'hard' and 'soft'-cutoff versions of ULI with 15 indicators (grouped destinations)
# Author:  Carl Higgs 
# Date:    20180411
#
# Submitted for publication as: 
# Higgs, C., Badland, H., Simons, K. and Giles-Corti, B. Urban liveability and adult cardiometabolic health: policy-relevant evidence from a cross-sectional Australian built environment data linkage study. npj Urban Sustainability (2021; submitted for review).

#  Postgresql MPI implementation steps for i indicators across j parcels
#  De Muro P., Mazziotta M., Pareto A. (2011), "Composite Indices of Development and Poverty: An Application to MDGs", Social Indicators Research, Volume 104, Number 1, pp. 1-18.
#  Vidoli, F., Fusco, E. Compind: Composite Indicators Functions, Version 1.1.2, 2016 
#  Adapted for postgresql by Carl Higgs, 4/4/2017

import os
import sys
import time
import psycopg2          # for database communication and management
import subprocess as sp  # for executing external commands (e.g. pgsql2shp)

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

# ULI schema to which this script pertains
#   -- created tables should be nested within this schema for tidiness and organisation
# detail: versions 2, with 15 indicators (grouped destinations)
# NOTE: original ULI's excluded parcels script was considered general enough to apply equally for ULI_v2_*, 
#       However, if future ULI versions are devised and prepared, the generation of schema specific
#       parcel exclusion tables should be created and drawn upon, catering to schema-specific indicator sets.
uli_schema = 'uli_v2_i15'

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create parcel-based liveability composite indicator for ULI schema {0}'.format(uli_schema)

A_pointsID = parser.get('parcels', 'parcel_id')

exclusion_criteria = 'WHERE  {0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(A_pointsID.lower())
parcelmb_exclusion_criteria = 'WHERE  parcelmb.{0} NOT IN (SELECT DISTINCT({0}) FROM excluded_parcels)'.format(A_pointsID.lower())

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlDBHost   = parser.get('postgresql', 'host')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()

# output folder for shape files
outpath = parser.get('data','folderPath')

# Create schema for this Urban Liveability Index version; 
createSchema = '''
  DROP SCHEMA IF EXISTS {0} CASCADE;
  CREATE SCHEMA {0};
  '''.format(uli_schema)
curs.execute(createSchema)
conn.commit()

# Define function to shape if variable is outlying  
createFunction = '''
  -- outlier limiting/compressing function
  -- if x < -2SD(x), scale up (hard knee upwards compression) to reach minimum by -3SD.
  -- if x > 2SD(x), scale up (hard knee downwards compression) to reach maximum by 3SD(x).
  
  CREATE OR REPLACE FUNCTION clean(var double precision,min_val double precision, max_val double precision, mean double precision, sd double precision) RETURNS double precision AS 
  $$
  DECLARE
  ll double precision := mean - 2*sd;
  ul double precision := mean + 2*sd;
  c  double precision :=  1*sd;
  BEGIN
    IF (min_val < ll-c) AND (var < ll) THEN 
      RETURN ll - c + c*(var - min_val)/(ll-min_val);
    ELSIF (max_val > ul+c) AND (var > ul) THEN 
      RETURN ul + c*(var - ul)/( max_val - ul );
    ELSE 
      RETURN var;
    END IF;
  END;
  $$
  LANGUAGE plpgsql
  RETURNS NULL ON NULL INPUT;
  '''
curs.execute(createFunction)
conn.commit()
print("Created custom function.")

# create destination group based indicators specific to this liveability schema

for i in ['hard','soft']:
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_groups_{1} ; 
  CREATE TABLE {3}.ind_groups_{1} AS
  SELECT {0},
         (COALESCE(communitycentre_1000m       , 0) +    
          COALESCE(museumartgallery_3200m      , 0) +    
          COALESCE(cinematheatre_3200m         , 0) +    
          COALESCE(libraries_2014_1000m        , 0)) / 4.0 AS community_culture_leisure,
         (COALESCE(childcareoutofschool_1600m  , 0) +
          COALESCE(childcare_800m              , 0)) / 2.0 AS early_years,
         (COALESCE(statesecondaryschools_1600m , 0) +
          COALESCE(stateprimaryschools_1600m   , 0)) / 2.0 AS education,
         (COALESCE(agedcare_2012_1000m         , 0) +
          COALESCE(communityhealthcentres_1000m, 0) +
          COALESCE(dentists_1000m              , 0) +
          COALESCE(gp_clinics_1000m            , 0) +
          COALESCE(maternalchildhealth_1000m   , 0) +
          COALESCE(pharmacy_1000m              , 0)) / 6.0 AS health_services,
         (COALESCE(swimmingpools_1200m         , 0) +
          COALESCE(sport_1200m                 , 0)) / 2.0 AS sport_rec,
         (COALESCE(supermarkets_1000m          , 0) +
          COALESCE(fishmeatpoultryshops_1600m  , 0) +
          COALESCE(fruitvegeshops_1600m        , 0)) / 3.0 AS food,
         (COALESCE(conveniencestores_1000m     , 0) +
          COALESCE(petrolstations_1000m        , 0) +
          COALESCE(newsagents_1000m            , 0)) / 3.0 AS convenience
         FROM ind_dest_{1}
         {2};
    '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created grouped indicator table '{1}.ind_groups_{0}'.".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_means_li_{1} ; 
  CREATE TABLE {3}.ind_summary_means_li_{1} AS
  SELECT (SELECT AVG(dd_nh1600m                   ) FROM  dwelling_density    {2}) AS dd_nh1600m                   ,
         (SELECT AVG(sc_nh1600m                   ) FROM  street_connectivity {2}) AS sc_nh1600m                   ,
         (SELECT AVG(pos_greq15000m2_in_400m_{1}  ) FROM  ind_pos             {2}) AS pos15000_access              ,
         (SELECT AVG(sa1_prop_affordablehous_30_40) FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40,
         (SELECT AVG(sa2_prop_live_work_sa3       ) FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3       ,
         (SELECT AVG(community_culture_leisure    ) FROM  {3}.ind_groups_{1}  {2}) AS community_culture_leisure    ,
         (SELECT AVG(early_years                  ) FROM  {3}.ind_groups_{1}  {2}) AS early_years                  ,
         (SELECT AVG(education                    ) FROM  {3}.ind_groups_{1}  {2}) AS education                    ,
         (SELECT AVG(health_services              ) FROM  {3}.ind_groups_{1}  {2}) AS health_services              ,
         (SELECT AVG(sport_rec                    ) FROM  {3}.ind_groups_{1}  {2}) AS sport_rec                    ,
         (SELECT AVG(food                         ) FROM  {3}.ind_groups_{1}  {2}) AS food                         ,
         (SELECT AVG(convenience                  ) FROM  {3}.ind_groups_{1}  {2}) AS convenience                  ,
         (SELECT AVG(busstop2012_400m             ) FROM  ind_dest_{1}        {2}) AS busstop2012_400m             ,
         (SELECT AVG(tramstops2012_600m           ) FROM  ind_dest_{1}        {2}) AS tramstops2012_600m           ,
         (SELECT AVG(trainstations2012_800m       ) FROM  ind_dest_{1}        {2}) AS trainstations2012_800m       ;
    '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_means_li_{0}', a summary of liveability indicator means.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_sd_li_{1} ;        
  CREATE TABLE {3}.ind_summary_sd_li_{1} AS
  SELECT (SELECT stddev_pop(dd_nh1600m                   ) FROM  dwelling_density    {2}) AS dd_nh1600m                   ,
         (SELECT stddev_pop(sc_nh1600m                   ) FROM  street_connectivity {2}) AS sc_nh1600m                   ,
         (SELECT stddev_pop(pos_greq15000m2_in_400m_{1}  ) FROM  ind_pos             {2}) AS pos15000_access              ,
         (SELECT stddev_pop(sa1_prop_affordablehous_30_40) FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40,
         (SELECT stddev_pop(sa2_prop_live_work_sa3       ) FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3       ,
         (SELECT stddev_pop(community_culture_leisure    ) FROM  {3}.ind_groups_{1}  {2}) AS community_culture_leisure    ,
         (SELECT stddev_pop(early_years                  ) FROM  {3}.ind_groups_{1}  {2}) AS early_years                  ,
         (SELECT stddev_pop(education                    ) FROM  {3}.ind_groups_{1}  {2}) AS education                    ,
         (SELECT stddev_pop(health_services              ) FROM  {3}.ind_groups_{1}  {2}) AS health_services              ,
         (SELECT stddev_pop(sport_rec                    ) FROM  {3}.ind_groups_{1}  {2}) AS sport_rec                    ,
         (SELECT stddev_pop(food                         ) FROM  {3}.ind_groups_{1}  {2}) AS food                         ,
         (SELECT stddev_pop(convenience                  ) FROM  {3}.ind_groups_{1}  {2}) AS convenience                  ,
         (SELECT stddev_pop(busstop2012_400m             ) FROM  ind_dest_{1}        {2}) AS busstop2012_400m             ,
         (SELECT stddev_pop(tramstops2012_600m           ) FROM  ind_dest_{1}        {2}) AS tramstops2012_600m           ,
         (SELECT stddev_pop(trainstations2012_800m       ) FROM  ind_dest_{1}        {2}) AS trainstations2012_800m       ;
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_sd_li_{0}', a summary of liveability indicator standard deviations.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_min_li_{1} ;        
  CREATE TABLE {3}.ind_summary_min_li_{1} AS
  SELECT (SELECT min(dd_nh1600m                   ) FROM  dwelling_density    {2}) AS dd_nh1600m                   ,
         (SELECT min(sc_nh1600m                   ) FROM  street_connectivity {2}) AS sc_nh1600m                   ,
         (SELECT min(pos_greq15000m2_in_400m_{1}  ) FROM  ind_pos             {2}) AS pos15000_access              ,
         (SELECT min(sa1_prop_affordablehous_30_40) FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40,
         (SELECT min(sa2_prop_live_work_sa3       ) FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3       ,
         (SELECT min(community_culture_leisure    ) FROM  {3}.ind_groups_{1}  {2}) AS community_culture_leisure    ,
         (SELECT min(early_years                  ) FROM  {3}.ind_groups_{1}  {2}) AS early_years                  ,
         (SELECT min(education                    ) FROM  {3}.ind_groups_{1}  {2}) AS education                    ,
         (SELECT min(health_services              ) FROM  {3}.ind_groups_{1}  {2}) AS health_services              ,
         (SELECT min(sport_rec                    ) FROM  {3}.ind_groups_{1}  {2}) AS sport_rec                    ,
         (SELECT min(food                         ) FROM  {3}.ind_groups_{1}  {2}) AS food                         ,
         (SELECT min(convenience                  ) FROM  {3}.ind_groups_{1}  {2}) AS convenience                  ,
         (SELECT min(busstop2012_400m             ) FROM  ind_dest_{1}        {2}) AS busstop2012_400m             ,
         (SELECT min(tramstops2012_600m           ) FROM  ind_dest_{1}        {2}) AS tramstops2012_600m           ,
         (SELECT min(trainstations2012_800m       ) FROM  ind_dest_{1}        {2}) AS trainstations2012_800m       ;
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_min_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_max_li_{1} ;        
  CREATE TABLE {3}.ind_summary_max_li_{1} AS
  SELECT (SELECT max(dd_nh1600m                   ) FROM  dwelling_density    {2}) AS dd_nh1600m                   ,
         (SELECT max(sc_nh1600m                   ) FROM  street_connectivity {2}) AS sc_nh1600m                   ,
         (SELECT max(pos_greq15000m2_in_400m_{1}  ) FROM  ind_pos             {2}) AS pos15000_access              ,
         (SELECT max(sa1_prop_affordablehous_30_40) FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40,
         (SELECT max(sa2_prop_live_work_sa3       ) FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3       ,
         (SELECT max(community_culture_leisure    ) FROM  {3}.ind_groups_{1}  {2}) AS community_culture_leisure    ,
         (SELECT max(early_years                  ) FROM  {3}.ind_groups_{1}  {2}) AS early_years                  ,
         (SELECT max(education                    ) FROM  {3}.ind_groups_{1}  {2}) AS education                    ,
         (SELECT max(health_services              ) FROM  {3}.ind_groups_{1}  {2}) AS health_services              ,
         (SELECT max(sport_rec                    ) FROM  {3}.ind_groups_{1}  {2}) AS sport_rec                    ,
         (SELECT max(food                         ) FROM  {3}.ind_groups_{1}  {2}) AS food                         ,
         (SELECT max(convenience                  ) FROM  {3}.ind_groups_{1}  {2}) AS convenience                  ,
         (SELECT max(busstop2012_400m             ) FROM  ind_dest_{1}        {2}) AS busstop2012_400m             ,
         (SELECT max(tramstops2012_600m           ) FROM  ind_dest_{1}        {2}) AS tramstops2012_600m           ,
         (SELECT max(trainstations2012_800m       ) FROM  ind_dest_{1}        {2}) AS trainstations2012_800m       ;
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_max_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_raw_ind_li_{1} ;        
  CREATE TABLE {3}.clean_raw_ind_li_{1} AS
  SELECT parcelmb.{0},
         abs_linkage.mb_code11,
         abs_linkage.sa1_7dig11,
         abs_linkage.sa2_name11,
         abs_linkage.sa3_name11,
         abs_linkage.ste_name11,
         non_abs_linkage.ssc_name,
         non_abs_linkage.lga_name11,
         clean(t3.dd_nh1600m                   ,_min.dd_nh1600m                   , _max.dd_nh1600m                   ,  _mean.dd_nh1600m                   ,_sd.dd_nh1600m                   ) AS dd_nh1600m                    ,
         clean(t4.sc_nh1600m                   ,_min.sc_nh1600m                   , _max.sc_nh1600m                   ,  _mean.sc_nh1600m                   ,_sd.sc_nh1600m                   ) AS sc_nh1600m                    ,
         clean(t7.pos_greq15000m2_in_400m_{1}  ,_min.pos15000_access              , _max.pos15000_access              ,  _mean.pos15000_access              ,_sd.pos15000_access              ) AS pos15000_access               ,
         clean(t0.sa1_prop_affordablehous_30_40,_min.sa1_prop_affordablehous_30_40, _max.sa1_prop_affordablehous_30_40,  _mean.sa1_prop_affordablehous_30_40,_sd.sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         clean(t0.sa2_prop_live_work_sa3       ,_min.sa2_prop_live_work_sa3       , _max.sa2_prop_live_work_sa3       ,  _mean.sa2_prop_live_work_sa3       ,_sd.sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        ,
         clean(t8.community_culture_leisure    ,_min.community_culture_leisure    , _max.community_culture_leisure    ,  _mean.community_culture_leisure    ,_sd.community_culture_leisure    ) AS community_culture_leisure    ,
         clean(t8.early_years                  ,_min.early_years                  , _max.early_years                  ,  _mean.early_years                  ,_sd.early_years                  ) AS early_years                  ,
         clean(t8.education                    ,_min.education                    , _max.education                    ,  _mean.education                    ,_sd.education                    ) AS education                    ,
         clean(t8.health_services              ,_min.health_services              , _max.health_services              ,  _mean.health_services              ,_sd.health_services              ) AS health_services              ,
         clean(t8.sport_rec                    ,_min.sport_rec                    , _max.sport_rec                    ,  _mean.sport_rec                    ,_sd.sport_rec                    ) AS sport_rec                    ,
         clean(t8.food                         ,_min.food                         , _max.food                         ,  _mean.food                         ,_sd.food                         ) AS food                         ,
         clean(t8.convenience                  ,_min.convenience                  , _max.convenience                  ,  _mean.convenience                  ,_sd.convenience                  ) AS convenience                  ,
         clean(t9.busstop2012_400m             ,_min.busstop2012_400m             , _max.busstop2012_400m             ,  _mean.busstop2012_400m             ,_sd.busstop2012_400m             ) AS busstop2012_400m             ,
         clean(t9.tramstops2012_600m           ,_min.tramstops2012_600m           , _max.tramstops2012_600m           ,  _mean.tramstops2012_600m           ,_sd.tramstops2012_600m           ) AS tramstops2012_600m           ,
         clean(t9.trainstations2012_800m       ,_min.trainstations2012_800m       , _max.trainstations2012_800m       ,  _mean.trainstations2012_800m       ,_sd.trainstations2012_800m       ) AS trainstations2012_800m       
    FROM parcelmb 
      LEFT JOIN abs_linkage                 ON parcelmb.mb_code11 = abs_linkage.mb_code11
      LEFT JOIN non_abs_linkage             ON parcelmb.{0} = non_abs_linkage.{0}        
      LEFT JOIN ind_abs               AS t0 ON parcelmb.{0} = t0.{0}                                    
      LEFT JOIN dwelling_density      AS t3 ON parcelmb.{0} = t3.{0}                   
      LEFT JOIN street_connectivity   AS t4 ON parcelmb.{0} = t4.{0}                                  
      LEFT JOIN ind_pos               AS t7 ON parcelmb.{0} = t7.{0}     
      LEFT JOIN {3}.ind_groups_{1}    AS t8 ON parcelmb.{0} = t8.{0}     
      LEFT JOIN ind_dest_{1}          AS t9 ON parcelmb.{0} = t9.{0},      
      {3}.ind_summary_means_li_{1}    AS _mean,
      {3}.ind_summary_sd_li_{1}       AS _sd,
      {3}.ind_summary_min_li_{1}      AS _min,
      {3}.ind_summary_max_li_{1}      AS _max
      {2} ;     
      ALTER TABLE {3}.clean_raw_ind_li_{1} ADD PRIMARY KEY ({0}); 
  '''.format(A_pointsID.lower(),i,parcelmb_exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_raw_ind_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_means_li_{1} ; 
  CREATE TABLE {3}.clean_ind_summary_means_li_{1} AS
  SELECT AVG(dd_nh1600m                   ) AS dd_nh1600m                    ,
         AVG(sc_nh1600m                   ) AS sc_nh1600m                    ,
         AVG(pos15000_access              ) AS pos15000_access               ,
         AVG(sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         AVG(sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        ,
         AVG(community_culture_leisure    ) AS community_culture_leisure     , 
         AVG(early_years                  ) AS early_years                   ,
         AVG(education                    ) AS education                     ,
         AVG(health_services              ) AS health_services               ,
         AVG(sport_rec                    ) AS sport_rec                     ,
         AVG(food                         ) AS food                          ,
         AVG(convenience                  ) AS convenience                   ,
         AVG(busstop2012_400m             ) AS busstop2012_400m              ,
         AVG(tramstops2012_600m           ) AS tramstops2012_600m            ,
         AVG(trainstations2012_800m       ) AS trainstations2012_800m        
         FROM {3}.clean_raw_ind_li_{1};
    '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_means_li_{0}', a summary of liveability indicator means.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_sd_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_sd_li_{1} AS
  SELECT stddev_pop(dd_nh1600m                   ) AS dd_nh1600m                    ,
         stddev_pop(sc_nh1600m                   ) AS sc_nh1600m                    ,
         stddev_pop(pos15000_access              ) AS pos15000_access               ,
         stddev_pop(sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         stddev_pop(sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        ,
         stddev_pop(community_culture_leisure    ) AS community_culture_leisure     , 
         stddev_pop(early_years                  ) AS early_years                   ,
         stddev_pop(education                    ) AS education                     ,
         stddev_pop(health_services              ) AS health_services               ,
         stddev_pop(sport_rec                    ) AS sport_rec                     ,
         stddev_pop(food                         ) AS food                          ,
         stddev_pop(convenience                  ) AS convenience                   ,
         stddev_pop(busstop2012_400m             ) AS busstop2012_400m              ,
         stddev_pop(tramstops2012_600m           ) AS tramstops2012_600m            ,
         stddev_pop(trainstations2012_800m       ) AS trainstations2012_800m        
         FROM {3}.clean_raw_ind_li_{1};       
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_sd_li_{0}', a summary of liveability indicator standard deviations.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_min_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_min_li_{1} AS
  SELECT min(dd_nh1600m                   ) AS dd_nh1600m                    ,
         min(sc_nh1600m                   ) AS sc_nh1600m                    ,
         min(pos15000_access              ) AS pos15000_access               ,
         min(sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         min(sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        ,
         min(community_culture_leisure    ) AS community_culture_leisure     , 
         min(early_years                  ) AS early_years                   ,
         min(education                    ) AS education                     ,
         min(health_services              ) AS health_services               ,
         min(sport_rec                    ) AS sport_rec                     ,
         min(food                         ) AS food                          ,
         min(convenience                  ) AS convenience                   ,
         min(busstop2012_400m             ) AS busstop2012_400m              ,
         min(tramstops2012_600m           ) AS tramstops2012_600m            ,
         min(trainstations2012_800m       ) AS trainstations2012_800m        
         FROM {3}.clean_raw_ind_li_{1};       
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_min_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_max_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_max_li_{1} AS
  SELECT max(dd_nh1600m                   ) AS dd_nh1600m                    ,
         max(sc_nh1600m                   ) AS sc_nh1600m                    ,
         max(pos15000_access              ) AS pos15000_access               ,
         max(sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         max(sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        ,
         max(community_culture_leisure    ) AS community_culture_leisure     , 
         max(early_years                  ) AS early_years                   ,
         max(education                    ) AS education                     ,
         max(health_services              ) AS health_services               ,
         max(sport_rec                    ) AS sport_rec                     ,
         max(food                         ) AS food                          ,
         max(convenience                  ) AS convenience                   ,
         max(busstop2012_400m             ) AS busstop2012_400m              ,
         max(tramstops2012_600m           ) AS tramstops2012_600m            ,
         max(trainstations2012_800m       ) AS trainstations2012_800m        
         FROM {3}.clean_raw_ind_li_{1};       
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_max_li_{0}'".format(i,uli_schema))

  
  createTable = '''
  -- Note that in this normalisation stage, indicator polarity is adjusted for: air pollution has values substracted from 100, whilst positive indicators have them added.
  --  ALSO note that walkability subindicators are processed here for completeness and comparison purposes -- these are not used in final LI calculation (other than as walkability components)
  DROP TABLE IF EXISTS {3}.clean_ind_mpi_norm_{1} ; 
  CREATE TABLE {3}.clean_ind_mpi_norm_{1} AS    
  SELECT {0},
         mb_code11,
         sa1_7dig11,
         sa2_name11,
         sa3_name11,
         ste_name11,
         ssc_name,
         lga_name11,
         100 + 10 * (t.dd_nh1600m                   - _mean.dd_nh1600m                    ) / _sd.dd_nh1600m                   ::double precision AS dd_nh1600m                  ,
         100 + 10 * (t.sc_nh1600m                   - _mean.sc_nh1600m                    ) / _sd.sc_nh1600m                   ::double precision AS sc_nh1600m                  ,
         100 + 10 * (t.pos15000_access              - _mean.pos15000_access               ) / _sd.pos15000_access              ::double precision AS pos15000_access             ,
         100 + 10 * (t.sa1_prop_affordablehous_30_40- _mean.sa1_prop_affordablehous_30_40 ) / _sd.sa1_prop_affordablehous_30_40::double precision AS sa1_prop_affordablehousing  ,
         100 + 10 * (t.sa2_prop_live_work_sa3       - _mean.sa2_prop_live_work_sa3        ) / _sd.sa2_prop_live_work_sa3       ::double precision AS sa2_prop_live_work_sa3      ,
         100 + 10 * (t.community_culture_leisure    - _mean.community_culture_leisure    ) / _sd.community_culture_leisure    ::double precision AS community_culture_leisure    ,
         100 + 10 * (t.early_years                  - _mean.early_years                  ) / _sd.early_years                  ::double precision AS early_years                  ,
         100 + 10 * (t.education                    - _mean.education                    ) / _sd.education                    ::double precision AS education                    ,
         100 + 10 * (t.health_services              - _mean.health_services              ) / _sd.health_services              ::double precision AS health_services              ,
         100 + 10 * (t.sport_rec                    - _mean.sport_rec                    ) / _sd.sport_rec                    ::double precision AS sport_rec                    ,
         100 + 10 * (t.food                         - _mean.food                         ) / _sd.food                         ::double precision AS food                         ,
         100 + 10 * (t.convenience                  - _mean.convenience                  ) / _sd.convenience                  ::double precision AS convenience                  ,
         100 + 10 * (t.busstop2012_400m             - _mean.busstop2012_400m             ) / _sd.busstop2012_400m             ::double precision AS busstop2012_400m             ,
         100 + 10 * (t.tramstops2012_600m           - _mean.tramstops2012_600m           ) / _sd.tramstops2012_600m           ::double precision AS tramstops2012_600m           ,
         100 + 10 * (t.trainstations2012_800m       - _mean.trainstations2012_800m       ) / _sd.trainstations2012_800m       ::double precision AS trainstations2012_800m       
  FROM {3}.clean_raw_ind_li_{1} AS t,
       {3}.clean_ind_summary_means_li_{1}  AS _mean,
       {3}.clean_ind_summary_sd_li_{1}  AS _sd;
  ALTER TABLE {3}.clean_ind_mpi_norm_{1} ADD PRIMARY KEY ({0});
  '''.format(A_pointsID.lower(),i,parcelmb_exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_mpi_norm_{0}', a table of MPI-normalised indicators.".format(i,uli_schema))
   
  createTable = ''' 
  -- 2. Create MPI estimates at parcel level
  -- rowmean*(1-(rowsd(z_j)/rowmean(z_j))^2) AS mpi_est_j
  -- took 1 minute for 2million vars
  DROP TABLE IF EXISTS {2}.clean_li_ci_{1}_est ; 
  CREATE TABLE {2}.clean_li_ci_{1}_est AS
  SELECT {0}, AVG(val) AS mean, stddev_pop(val) AS sd, stddev_pop(val)/AVG(val) AS cv, AVG(val)-(stddev_pop(val)^2)/AVG(val) AS li_ci_est 
  FROM (SELECT {0}, 
               unnest(array[dd_nh1600m,sc_nh1600m,pos15000_access,sa1_prop_affordablehousing,sa2_prop_live_work_sa3,community_culture_leisure,early_years,education,health_services,sport_rec,food,convenience,busstop2012_400m,tramstops2012_600m,trainstations2012_800m]) as val 
        FROM {2}.clean_ind_mpi_norm_{1} ) alias
  GROUP BY {0};
  '''.format(A_pointsID.lower(),i,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_li_ci_{0}_est', a parcel level composite indicator estimate for liveability.".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {2}.clean_li_parcel_ci_{1} ; 
  CREATE TABLE {2}.clean_li_parcel_ci_{1} AS
  SELECT {2}.clean_ind_mpi_norm_{1}.{0},
         mb_code11,
         sa1_7dig11,
         sa2_name11,
         sa3_name11,
         ssc_name,
         lga_name11,
         ste_name11,
         li_ci_est,
         dd_nh1600m  ,
         sc_nh1600m  ,
         pos15000_access,
         sa1_prop_affordablehousing,
         sa2_prop_live_work_sa3,
         community_culture_leisure    ,
         early_years                  ,
         education                    ,
         health_services              ,
         sport_rec                    ,
         food                         ,
         convenience                  ,
         busstop2012_400m             ,
         tramstops2012_600m           ,
         trainstations2012_800m       
  FROM {2}.clean_ind_mpi_norm_{1}
  LEFT JOIN {2}.clean_li_ci_{1}_est  ON {2}.clean_li_ci_{1}_est.{0} = {2}.clean_ind_mpi_norm_{1}.{0};
  ALTER TABLE {2}.clean_li_parcel_ci_{1} ADD PRIMARY KEY ({0});
  -- export for analysis, although this may be better achieved through sql queries by SA1
      -- same effect but lighter on memory
  -- COPY {2}.clean_li_parcel_ci_{1} TO 'C:/data/liveability/data/{2}_li_parcel_ci_{1}.csv' DELIMITER ',' CSV HEADER;
  '''.format(A_pointsID.lower(),i,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_li_parcel_ci_{0}', a summary table combining the parcel level composite indicator estimate for for liveability with associated regions, and the standardised indicators of which the pCI is comprised of.".format(i,uli_schema))
  
  # create raw indicator table
  createTable = '''
  DROP TABLE IF EXISTS {3}.raw_indicators_{1} ; 
  CREATE TABLE {3}.raw_indicators_{1} AS    
  SELECT parcelmb.{0},
             abs_linkage.mb_code11,
             abs_linkage.sa1_7dig11,
             abs_linkage.sa2_name11,
             abs_linkage.sa3_name11,
             abs_linkage.ste_name11,
             non_abs_linkage.ssc_name,
             non_abs_linkage.lga_name11,
             li_ci_est,
             dd_nh1600m,
             sc_nh1600m,
             pos_greq15000m2_in_400m_{1} AS pos15000_access,
             sa1_prop_affordablehous_30_40 AS sa1_prop_affordablehousing,
             sa2_prop_live_work_sa3,
             community_culture_leisure    ,
             early_years                  ,
             education                    ,
             health_services              ,
             sport_rec                    ,
             food                         ,
             convenience                  ,
             busstop2012_400m             ,
             tramstops2012_600m           ,
             trainstations2012_800m       
  FROM parcelmb 
  LEFT JOIN abs_linkage ON parcelmb.mb_code11 = abs_linkage.mb_code11
  LEFT JOIN non_abs_linkage ON parcelmb.{0} = non_abs_linkage.{0}
  LEFT JOIN {3}.clean_li_ci_{1}_est ON {3}.clean_li_ci_{1}_est.{0} = parcelmb.{0}
  LEFT JOIN ind_abs ON parcelmb.{0} = ind_abs.{0}
  LEFT JOIN dwelling_density ON parcelmb.{0} = dwelling_density.{0}
  LEFT JOIN street_connectivity ON parcelmb.{0} = street_connectivity.{0}
  LEFT JOIN ind_pos ON parcelmb.{0} = ind_pos.{0}
  LEFT JOIN {3}.ind_groups_{1} ON parcelmb.{0} = {3}.ind_groups_{1}.{0}
  LEFT JOIN ind_dest_{1} ON parcelmb.{0} = ind_dest_{1}.{0}
  {2};
  ALTER TABLE {3}.raw_indicators_{1} ADD PRIMARY KEY ({0});
  '''.format(A_pointsID.lower(),i,parcelmb_exclusion_criteria,uli_schema)

  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.raw_indicators_{0}', with parcel level id, linkage codes, pLI estimates, and raw indicators".format(i,uli_schema))  

for type in ['hard','soft']:
  createTable = '''
  DROP TABLE IF EXISTS {1}.clean_li_percentile_{0};
  CREATE TABLE {1}.clean_li_percentile_{0} AS
  SELECT t1.detail_pid,
         round(100*cume_dist() OVER(ORDER BY li_ci_est)::numeric,0) as li_ci_est,
         geom
  FROM {1}.clean_li_parcel_ci_{0} AS t1
  LEFT JOIN parcel_xy AS t2 on t1.detail_pid = t2.detail_pid
  '''.format(type,uli_schema)

  curs.execute(createTable)
  conn.commit()
  print("Created {0} address-level percentiles for schema {1}".format(type,uli_schema))    
  
# create sa1 area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.sa1_area;
  CREATE TABLE {0}.sa1_area AS
  SELECT sa1_7dig11, 
  string_agg(distinct(ssc_name),',') AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  WHERE sa1_7dig11 IN (SELECT sa1_7dig11 FROM abs_2011_irsd)
  GROUP BY sa1_7dig11
  ORDER BY sa1_7dig11 ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()

# create sa2 area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.sa2_area;
  CREATE TABLE {0}.sa2_area AS
  SELECT sa2_name11, 
  string_agg(distinct(ssc_name),',') AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  WHERE sa2_name11 IN (SELECT sa2_name11 FROM abs_2011_irsd)
  GROUP BY sa2_name11
  ORDER BY sa2_name11 ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()


# create Suburb area linkage corresponding to later SA1 aggregate tables
createTable = '''  
  DROP TABLE IF EXISTS {0}.ssc_area;
  CREATE TABLE {0}.ssc_area AS
  SELECT DISTINCT(ssc_name) AS suburb, 
  string_agg(distinct(lga_name11), ', ') AS lga
  FROM  {0}.raw_indicators_hard
  GROUP BY ssc_name
  ORDER BY ssc_name ASC;
  '''.format(uli_schema)
curs.execute(createTable)
conn.commit()
  
# create aggregated raw liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_raw_{1}_{0} ; 
    CREATE TABLE {2}.li_raw_{1}_{0} AS
    SELECT {0},
      AVG(li_ci_est                   ) AS li_ci_est                    ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                   ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                   ,
      AVG(pos15000_access             ) AS pos15000_access              ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing   ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3       ,
      AVG(community_culture_leisure   ) AS community_culture_leisure    ,
      AVG(early_years                 ) AS early_years                  ,
      AVG(education                   ) AS education                    ,
      AVG(health_services             ) AS health_services              ,
      AVG(sport_rec                   ) AS sport_rec                    ,
      AVG(food                        ) AS food                         ,
      AVG(convenience                 ) AS convenience                  ,
      AVG(busstop2012_400m            ) AS busstop2012_400m             ,
      AVG(tramstops2012_600m          ) AS tramstops2012_600m           ,
      AVG(trainstations2012_800m      ) AS trainstations2012_800m       
      FROM {2}.raw_indicators_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_raw_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)
    curs.execute(createTable)
    conn.commit()
    print("Created raw {1} averages at {0} level for schema {2}".format(area,type,uli_schema))   
    
# create aggregated SD for raw liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_raw_sd_{1}_{0} ; 
    CREATE TABLE {2}.li_raw_sd_{1}_{0} AS
    SELECT {0},
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                     ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                    ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                    ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access               ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing    ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3        ,
      stddev_pop(community_culture_leisure    ) AS sd_community_culture_leisure    ,
      stddev_pop(early_years                  ) AS sd_early_years                  ,
      stddev_pop(education                    ) AS sd_education                    ,
      stddev_pop(health_services              ) AS sd_health_services              ,
      stddev_pop(sport_rec                    ) AS sd_sport_rec                    ,
      stddev_pop(food                         ) AS sd_food                         ,
      stddev_pop(convenience                  ) AS sd_convenience                  ,
      stddev_pop(busstop2012_400m             ) AS sd_busstop2012_400m             ,
      stddev_pop(tramstops2012_600m           ) AS sd_tramstops2012_600m           ,
      stddev_pop(trainstations2012_800m       ) AS sd_trainstations2012_800m       
      FROM  {2}.raw_indicators_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_raw_sd_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    

    curs.execute(createTable)
    conn.commit()
    print("Created SD for raw {1} averages at {0} level for schema {2}".format(area,type,uli_schema))   


# create aggregated raw liveability range for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_range_{1}_{0} ; 
    CREATE TABLE {2}.li_range_{1}_{0} AS
    SELECT {0},
      round(min(li_centile                       )::numeric,1)::text || ' - ' ||round(max(li_centile                       )::numeric,1)::text AS li_centile                   ,
      round(min(dd_nh1600m                       )::numeric,1)::text || ' - ' ||round(max(dd_nh1600m                       )::numeric,1)::text AS dd_nh1600m                   ,
      round(min(sc_nh1600m                       )::numeric,1)::text || ' - ' ||round(max(sc_nh1600m                       )::numeric,1)::text AS sc_nh1600m                   ,
      round(min(100*pos15000_access              )::numeric,1)::text || ' - ' ||round(max(100*pos15000_access              )::numeric,1)::text AS pos15000_access              ,
      round(min(100*sa1_prop_affordablehousing   )::numeric,1)::text || ' - ' ||round(max(100*sa1_prop_affordablehousing   )::numeric,1)::text AS sa1_prop_affordablehousing   ,
      round(min(100*sa2_prop_live_work_sa3       )::numeric,1)::text || ' - ' ||round(max(100*sa2_prop_live_work_sa3       )::numeric,1)::text AS sa2_prop_live_work_sa3       ,  
      round(min(100*community_culture_leisure    )::numeric,1)::text || ' - ' ||round(max(100*community_culture_leisure    )::numeric,1)::text AS community_culture_leisure    ,  
      round(min(100*early_years                  )::numeric,1)::text || ' - ' ||round(max(100*early_years                  )::numeric,1)::text AS early_years                  ,  
      round(min(100*education                    )::numeric,1)::text || ' - ' ||round(max(100*education                    )::numeric,1)::text AS education                    ,  
      round(min(100*health_services              )::numeric,1)::text || ' - ' ||round(max(100*health_services              )::numeric,1)::text AS health_services              ,  
      round(min(100*sport_rec                    )::numeric,1)::text || ' - ' ||round(max(100*sport_rec                    )::numeric,1)::text AS sport_rec                    ,  
      round(min(100*food                         )::numeric,1)::text || ' - ' ||round(max(100*food                         )::numeric,1)::text AS food                         ,  
      round(min(100*convenience                  )::numeric,1)::text || ' - ' ||round(max(100*convenience                  )::numeric,1)::text AS convenience                  ,  
      round(min(100*busstop2012_400m             )::numeric,1)::text || ' - ' ||round(max(100*busstop2012_400m             )::numeric,1)::text AS busstop2012_400m             ,  
      round(min(100*tramstops2012_600m           )::numeric,1)::text || ' - ' ||round(max(100*tramstops2012_600m           )::numeric,1)::text AS tramstops2012_600m           ,  
      round(min(100*trainstations2012_800m       )::numeric,1)::text || ' - ' ||round(max(100*trainstations2012_800m       )::numeric,1)::text AS trainstations2012_800m         
      FROM {2}.raw_indicators_{1}  AS t1
      LEFT JOIN
      (SELECT detail_pid, 
              100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile
       FROM {2}.clean_li_parcel_ci_{1}) AS t2 ON t1.detail_pid = t2.detail_pid
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_range_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)
    curs.execute(createTable)
    conn.commit()
    print("Created raw {1} range at {0} level for schema {2}".format(area,type,uli_schema))
    
# create aggregated raw liveability most for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.li_most_{1}_{0} ; 
    CREATE TABLE {2}.li_most_{1}_{0} AS
    SELECT {0},
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY li_centile                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY li_centile                  )::numeric,1)::text AS li_centile                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY dd_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY dd_nh1600m                  )::numeric,1)::text AS dd_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY sc_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY sc_nh1600m                  )::numeric,1)::text AS sc_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*pos15000_access             )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*pos15000_access         )::numeric,1)::text AS pos15000_access             ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text AS sa1_prop_affordablehousing,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3  )::numeric,1)::text AS sa2_prop_live_work_sa3,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*community_culture_leisure       )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*community_culture_leisure    )::numeric,1)::text AS community_culture_leisure    ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*early_years      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*early_years                  )::numeric,1)::text AS early_years                  ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*education         )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*education                    )::numeric,1)::text AS education                    ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*health_services        )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*health_services              )::numeric,1)::text AS health_services              ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sport_rec  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sport_rec                    )::numeric,1)::text AS sport_rec                    ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*food              )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*food                         )::numeric,1)::text AS food                         ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*convenience )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*convenience                  )::numeric,1)::text AS convenience                  ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*busstop2012_400m            )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*busstop2012_400m             )::numeric,1)::text AS busstop2012_400m             ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*tramstops2012_600m          )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*tramstops2012_600m           )::numeric,1)::text AS tramstops2012_600m           ,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*trainstations2012_800m      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*trainstations2012_800m       )::numeric,1)::text AS trainstations2012_800m         
      FROM {2}.raw_indicators_{1} AS t1
      LEFT JOIN 
      (SELECT detail_pid, 
              100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile
       FROM {2}.clean_li_parcel_ci_{1}) AS t2 ON t1.detail_pid = t2.detail_pid
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.li_most_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)
    curs.execute(createTable)
    conn.commit()
    print("Created raw {1} most at {0} level for schema {2}".format(area,type,uli_schema))
    
    
# create aggregated normalised liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_mpi_norm_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_mpi_norm_{1}_{0} AS
    SELECT {0},
      AVG(li_ci_est                   ) AS li_ci_est                     ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                    ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                    ,
      AVG(pos15000_access             ) AS pos15000_access               ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing    ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3        ,
      AVG(community_culture_leisure    ) AS community_culture_leisure    ,
      AVG(early_years                  ) AS early_years                  ,
      AVG(education                    ) AS education                    ,
      AVG(health_services              ) AS health_services              ,
      AVG(sport_rec                    ) AS sport_rec                    ,
      AVG(food                         ) AS food                         ,
      AVG(convenience                  ) AS convenience                  ,
      AVG(busstop2012_400m             ) AS busstop2012_400m             ,
      AVG(tramstops2012_600m           ) AS tramstops2012_600m           ,
      AVG(trainstations2012_800m       ) AS trainstations2012_800m       
      FROM  {2}.clean_li_parcel_ci_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_mpi_norm_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    
    
    curs.execute(createTable)
    conn.commit()
    print("Created normalised {1} averages at {0} level for schema {2}".format(area,type,uli_schema))  

# create aggregated SD for normalised liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_mpi_sd_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_mpi_sd_{1}_{0} AS
    SELECT {0},
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                    ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                   ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                   ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access              ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing   ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3       ,
      stddev_pop(community_culture_leisure   ) AS sd_community_culture_leisure    ,
      stddev_pop(early_years                 ) AS sd_early_years                  ,
      stddev_pop(education                   ) AS sd_education                    ,
      stddev_pop(health_services             ) AS sd_health_services              ,
      stddev_pop(sport_rec                   ) AS sd_sport_rec                    ,
      stddev_pop(food                        ) AS sd_food                         ,
      stddev_pop(convenience                 ) AS sd_convenience                  ,
      stddev_pop(busstop2012_400m            ) AS sd_busstop2012_400m             ,
      stddev_pop(tramstops2012_600m          ) AS sd_tramstops2012_600m           ,
      stddev_pop(trainstations2012_800m      ) AS sd_trainstations2012_800m       
      FROM  {2}.clean_li_parcel_ci_{1}
      GROUP BY {0}
      ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_mpi_sd_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)    
    
    curs.execute(createTable)
    conn.commit()
    print("Created SD for normalised {1} averages at {0} level for schema {2}".format(area,type,uli_schema))  
    
# create deciles of liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_deciles_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_deciles_{1}_{0} AS
    SELECT {0},
           round(10*cume_dist() OVER(ORDER BY li_ci_est                   )::numeric,0) as li_ci_est                   ,
           round(10*cume_dist() OVER(ORDER BY dd_nh1600m                  )::numeric,0) as dd_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY sc_nh1600m                  )::numeric,0) as sc_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY pos15000_access             )::numeric,0) as pos15000_access             ,
           round(10*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing  )::numeric,0) as sa1_prop_affordablehousing  ,
           round(10*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3      )::numeric,0) as sa2_prop_live_work_sa3      ,
           round(10*cume_dist() OVER(ORDER BY community_culture_leisure   )::numeric,0) as community_culture_leisure   ,
           round(10*cume_dist() OVER(ORDER BY early_years                 )::numeric,0) as early_years                 ,
           round(10*cume_dist() OVER(ORDER BY education                   )::numeric,0) as education                   ,
           round(10*cume_dist() OVER(ORDER BY health_services             )::numeric,0) as health_services             ,
           round(10*cume_dist() OVER(ORDER BY sport_rec                   )::numeric,0) as sport_rec                   ,
           round(10*cume_dist() OVER(ORDER BY food                        )::numeric,0) as food                        ,
           round(10*cume_dist() OVER(ORDER BY convenience                 )::numeric,0) as convenience                 ,
           round(10*cume_dist() OVER(ORDER BY busstop2012_400m            )::numeric,0) as busstop2012_400m            ,
           round(10*cume_dist() OVER(ORDER BY tramstops2012_600m          )::numeric,0) as tramstops2012_600m          ,
           round(10*cume_dist() OVER(ORDER BY trainstations2012_800m      )::numeric,0) as trainstations2012_800m      
    FROM {2}.clean_li_mpi_norm_{1}_{0}
    ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_deciles_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)

    curs.execute(createTable)
    conn.commit()
    print("Created {1} deciles at {0} level for schema {2}".format(area,type,uli_schema))  

# create percentiles of liveability estimates for selected area
for type in ['hard','soft']:
  for area in ['mb_code11','sa1_7dig11','sa2_name11','ssc_name','lga_name11']:
    createTable = '''
    DROP TABLE IF EXISTS {2}.clean_li_percentiles_{1}_{0} ; 
    CREATE TABLE {2}.clean_li_percentiles_{1}_{0} AS
    SELECT {0},
           round(100*cume_dist() OVER(ORDER BY li_ci_est                    )::numeric,0) as li_ci_est                    ,
           round(100*cume_dist() OVER(ORDER BY dd_nh1600m                   )::numeric,0) as dd_nh1600m                   ,
           round(100*cume_dist() OVER(ORDER BY sc_nh1600m                   )::numeric,0) as sc_nh1600m                   ,
           round(100*cume_dist() OVER(ORDER BY pos15000_access              )::numeric,0) as pos15000_access              ,
           round(100*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing   )::numeric,0) as sa1_prop_affordablehousing   ,
           round(100*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3       )::numeric,0) as sa2_prop_live_work_sa3       ,
           round(100*cume_dist() OVER(ORDER BY community_culture_leisure    )::numeric,0) as community_culture_leisure    ,
           round(100*cume_dist() OVER(ORDER BY early_years                  )::numeric,0) as early_years                  ,
           round(100*cume_dist() OVER(ORDER BY education                    )::numeric,0) as education                    ,
           round(100*cume_dist() OVER(ORDER BY health_services              )::numeric,0) as health_services              ,
           round(100*cume_dist() OVER(ORDER BY sport_rec                    )::numeric,0) as sport_rec                    ,
           round(100*cume_dist() OVER(ORDER BY food                         )::numeric,0) as food                         ,
           round(100*cume_dist() OVER(ORDER BY convenience                  )::numeric,0) as convenience                  ,
           round(100*cume_dist() OVER(ORDER BY busstop2012_400m             )::numeric,0) as busstop2012_400m             ,
           round(100*cume_dist() OVER(ORDER BY tramstops2012_600m           )::numeric,0) as tramstops2012_600m           ,
           round(100*cume_dist() OVER(ORDER BY trainstations2012_800m       )::numeric,0) as trainstations2012_800m       
    FROM {2}.clean_li_mpi_norm_{1}_{0} 
    ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_percentiles_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)

    curs.execute(createTable)
    conn.commit()
    print("Created {1} percentiles at {0} level for schema {2}".format(area,type,uli_schema))  

# output to completion log    
script_running_log(script, task, start)

