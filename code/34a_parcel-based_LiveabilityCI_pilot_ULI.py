# Purpose: create parcel-based liveability composite indicator
#          In particular, 'hard' and 'soft'-cutoff versions of:
#             daily living, local living and social infrastructure mix
# Author:  Carl Higgs 
# Date:    20170519 (originally authored; updated with publishing details 12 July 2021)
#
# This is the code to calculate the Pilot ULI published as: 
# Higgs, C., Badland, H., Simons, K. et al. The Urban Liveability Index: developing a policy-relevant urban liveability composite measure and evaluating associations with transport mode choice. Int J Health Geogr 18, 14 (2019). https://doi.org/10.1186/s12942-019-0178-8
#
#  Postgresql MPI implementation steps for i indicators across j parcels
#  De Muro P., Mazziotta M., Pareto A. (2011), "Composite Indices of Development and Poverty: An Application to MDGs", Social Indicators Research, Volume 104, Number 1, pp. 1-18.
#  Vidoli, F., Fusco, E. Compind: Composite Indicators Functions, Version 1.1.2, 2016 
#  Adapted for postgresql by Carl Higgs, 4/4/2017
# 
#  'COPY' statements are included below as indicative examples; adapt to relevant output url
#  alternatively, results can be queried direct from Postgresql database
#  These statement are commented out, as they will not run from the script environment.
#  Run them as an interactive query e.g. in pgAdmin or using pgsql
# 
#  1. Create table of MPI-normalised variables
#   100+10*(x_i-mean(x_i))/sd(x_i)::double precision AS z_ij
#  This took 2 minutes for 2,018,305 parcels
#  NOTE: 
#    - using population standard deviation, as we are using 'population' of parcels, not sample in this context
#    - ASSUMPTION THAT INDICATORS ARE ALL POSITIVE!!!! 
#        -- no negative numbers in this... does not work..  
#        -- perhaps introduce case clause to min-max normalise such instances
#                -- this is what I have done for 'walkability'


# Key revisions:
#  - An alternate version of the ULI is also created EXCLUDING AIR QUALITY INDICATORS as a basis for sensitivity analysis. [CH 27 February 2018]
#  - Updated for better organisation, with tables specific to a particular ULI implementation contained within a ULI-specific schema.  For version 1 (this script), this includes ULI with and without air quality.  Future versions will seperate design elements such as this to seperate schemas. [CH 11 April 2018]


import os
import sys
import time
import psycopg2             # for database communication and management
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


# ULI schema to which this script pertains
#   -- created tables should be nested within this schema for tidiness and organisation
# detail: versions 1, with 7 or 6 indicators (with or without air quality)
uli_schema = 'uli_v1'

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

# NOTE: normalised scores are calculated for dwelling density, street connectivity and daily living for completeness
#       however, these are subsumed within the walkability construct for purposes of composite indicator construction


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

for i in ['hard','soft']:
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_means_li_{1} ; 
  CREATE TABLE {3}.ind_summary_means_li_{1} AS
  SELECT (SELECT AVG(walkability)                     FROM  ind_walkability_{1} {2}) AS walkability                   ,
         (SELECT AVG(daily_living)                    FROM  ind_daily_living_{1} {2}) AS daily_living,
         (SELECT AVG(dd_nh1600m)                      FROM  dwelling_density {2})    AS dd_nh1600m,
         (SELECT AVG(sc_nh1600m)                      FROM  street_connectivity {2}) AS sc_nh1600m,
         (SELECT AVG(si_mix)                          FROM  ind_si_mix_{1}      {2}) AS si_mix                        ,
         (SELECT AVG(dest_pt)                         FROM  ind_dest_pt_{1}     {2}) AS dest_pt                       ,
         (SELECT AVG(pos_greq15000m2_in_400m_{1})     FROM  ind_pos             {2}) AS pos15000_access               ,
         (SELECT AVG(pred_no2_2011_col_ppb)           FROM  parcelmb LEFT JOIN no2_pred ON parcelmb.mb_code11 = no2_pred.mb_code11  {2}) AS pred_no2_2011_col_ppb,
         (SELECT AVG(sa1_prop_affordablehous_30_40)   FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40 ,
         (SELECT AVG(sa2_prop_live_work_sa3)          FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3        ;
    '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_means_li_{0}', a summary of liveability indicator means.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_sd_li_{1} ;        
  CREATE TABLE {3}.ind_summary_sd_li_{1} AS
  SELECT (SELECT stddev_pop(walkability)                     FROM  ind_walkability_{1} {2}) AS walkability                   ,
         (SELECT stddev_pop(daily_living)                    FROM  ind_daily_living_{1} {2}) AS daily_living ,
         (SELECT stddev_pop(dd_nh1600m)                      FROM  dwelling_density {2})     AS dd_nh1600m   ,
         (SELECT stddev_pop(sc_nh1600m)                      FROM  street_connectivity {2})  AS sc_nh1600m   ,
         (SELECT stddev_pop(si_mix)                          FROM  ind_si_mix_{1}      {2}) AS si_mix                        ,
         (SELECT stddev_pop(dest_pt)                         FROM  ind_dest_pt_{1}     {2}) AS dest_pt                       ,
         (SELECT stddev_pop(pos_greq15000m2_in_400m_{1})     FROM  ind_pos             {2}) AS pos15000_access               ,
         (SELECT stddev_pop(pred_no2_2011_col_ppb)           FROM  parcelmb LEFT JOIN no2_pred ON parcelmb.mb_code11 = no2_pred.mb_code11 {2}) AS pred_no2_2011_col_ppb,  
         (SELECT stddev_pop(sa1_prop_affordablehous_30_40)   FROM  ind_abs             {2}) AS sa1_prop_affordablehous_30_40 ,
         (SELECT stddev_pop(sa2_prop_live_work_sa3)          FROM  ind_abs             {2}) AS sa2_prop_live_work_sa3        ;
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_sd_li_{0}', a summary of liveability indicator standard deviations.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_min_li_{1} ;        
  CREATE TABLE {3}.ind_summary_min_li_{1} AS
  SELECT (SELECT min(walkability)                     FROM  ind_walkability_{1}  {2}) AS walkability                   ,
         (SELECT min(daily_living)                    FROM  ind_daily_living_{1} {2}) AS daily_living                  ,
         (SELECT min(dd_nh1600m)                      FROM  dwelling_density     {2}) AS dd_nh1600m                ,
         (SELECT min(sc_nh1600m)                      FROM  street_connectivity  {2}) AS sc_nh1600m                   ,
         (SELECT min(si_mix)                          FROM  ind_si_mix_{1}       {2}) AS si_mix                        ,
         (SELECT min(dest_pt)                         FROM  ind_dest_pt_{1}      {2}) AS dest_pt                       ,
         (SELECT min(pos_greq15000m2_in_400m_{1})     FROM  ind_pos              {2}) AS pos15000_access               ,
         (SELECT min(pred_no2_2011_col_ppb)           FROM  parcelmb LEFT JOIN no2_pred ON parcelmb.mb_code11 = no2_pred.mb_code11 {2}) AS pred_no2_2011_col_ppb,  
         (SELECT min(sa1_prop_affordablehous_30_40)   FROM  ind_abs              {2}) AS sa1_prop_affordablehous_30_40 ,
         (SELECT min(sa2_prop_live_work_sa3)          FROM  ind_abs              {2}) AS sa2_prop_live_work_sa3        ;
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.ind_summary_min_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.ind_summary_max_li_{1} ;        
  CREATE TABLE {3}.ind_summary_max_li_{1} AS
  SELECT (SELECT max(walkability)                     FROM  ind_walkability_{1}  {2}) AS walkability                   ,
         (SELECT max(daily_living)                    FROM  ind_daily_living_{1} {2}) AS daily_living                  ,
         (SELECT max(dd_nh1600m)                      FROM  dwelling_density     {2}) AS dd_nh1600m                ,
         (SELECT max(sc_nh1600m)                      FROM  street_connectivity  {2}) AS sc_nh1600m                   ,
         (SELECT max(si_mix)                          FROM  ind_si_mix_{1}       {2}) AS si_mix                        ,
         (SELECT max(dest_pt)                         FROM  ind_dest_pt_{1}      {2}) AS dest_pt                       ,
         (SELECT max(pos_greq15000m2_in_400m_{1})     FROM  ind_pos              {2}) AS pos15000_access               ,
         (SELECT max(pred_no2_2011_col_ppb)           FROM  parcelmb LEFT JOIN no2_pred ON parcelmb.mb_code11 = no2_pred.mb_code11 {2}) AS pred_no2_2011_col_ppb,  
         (SELECT max(sa1_prop_affordablehous_30_40)   FROM  ind_abs              {2}) AS sa1_prop_affordablehous_30_40 ,
         (SELECT max(sa2_prop_live_work_sa3)          FROM  ind_abs              {2}) AS sa2_prop_live_work_sa3        ;
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
         clean(t1.walkability                  ,_min.walkability                  , _max.walkability                  ,  _mean.walkability                  ,_sd.walkability                  ) AS walkability                   ,
         clean(t2.daily_living                 ,_min.daily_living                 , _max.daily_living                 ,  _mean.daily_living                 ,_sd.daily_living                 ) AS daily_living                  ,
         clean(t3.dd_nh1600m                   ,_min.dd_nh1600m                   , _max.dd_nh1600m                   ,  _mean.dd_nh1600m                   ,_sd.dd_nh1600m                   ) AS dd_nh1600m                    ,
         clean(t4.sc_nh1600m                   ,_min.sc_nh1600m                   , _max.sc_nh1600m                   ,  _mean.sc_nh1600m                   ,_sd.sc_nh1600m                   ) AS sc_nh1600m                    ,
         clean(t5.si_mix                       ,_min.si_mix                       , _max.si_mix                       ,  _mean.si_mix                       ,_sd.si_mix                       ) AS si_mix                        ,
         clean(t6.dest_pt                      ,_min.dest_pt                      , _max.dest_pt                      ,  _mean.dest_pt                      ,_sd.dest_pt                      ) AS dest_pt                       ,
         clean(t7.pos_greq15000m2_in_400m_{1}  ,_min.pos15000_access              , _max.pos15000_access              ,  _mean.pos15000_access              ,_sd.pos15000_access              ) AS pos15000_access               ,
         clean(t8.pred_no2_2011_col_ppb        ,_min.pred_no2_2011_col_ppb        , _max.pred_no2_2011_col_ppb        ,  _mean.pred_no2_2011_col_ppb        ,_sd.pred_no2_2011_col_ppb        ) AS pred_no2_2011_col_ppb         ,  
         clean(t0.sa1_prop_affordablehous_30_40,_min.sa1_prop_affordablehous_30_40, _max.sa1_prop_affordablehous_30_40,  _mean.sa1_prop_affordablehous_30_40,_sd.sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         clean(t0.sa2_prop_live_work_sa3       ,_min.sa2_prop_live_work_sa3       , _max.sa2_prop_live_work_sa3       ,  _mean.sa2_prop_live_work_sa3       ,_sd.sa2_prop_live_work_sa3       ) AS sa2_prop_live_work_sa3        
    FROM parcelmb 
      LEFT JOIN abs_linkage                 ON parcelmb.mb_code11 = abs_linkage.mb_code11
      LEFT JOIN non_abs_linkage             ON parcelmb.{0} = non_abs_linkage.{0}        
      LEFT JOIN ind_abs               AS t0 ON parcelmb.{0} = t0.{0}                   
      LEFT JOIN ind_walkability_{1}   AS t1 ON parcelmb.{0} = t1.{0}                   
      LEFT JOIN ind_daily_living_{1}  AS t2 ON parcelmb.{0} = t2.{0}                   
      LEFT JOIN dwelling_density      AS t3 ON parcelmb.{0} = t3.{0}                   
      LEFT JOIN street_connectivity   AS t4 ON parcelmb.{0} = t4.{0}                   
      LEFT JOIN ind_si_mix_{1}        AS t5 ON parcelmb.{0} = t5.{0}                   
      LEFT JOIN ind_dest_pt_{1}       AS t6 ON parcelmb.{0} = t6.{0}                   
      LEFT JOIN ind_pos               AS t7 ON parcelmb.{0} = t7.{0}                   
      LEFT JOIN no2_pred              AS t8 ON parcelmb.mb_code11 = t8.mb_code11,    
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
  SELECT AVG(walkability)                   AS walkability                   ,
         AVG(daily_living)                  AS daily_living                  ,
         AVG(dd_nh1600m)                    AS dd_nh1600m                    ,
         AVG(sc_nh1600m)                    AS sc_nh1600m                    ,
         AVG(si_mix)                        AS si_mix                        ,
         AVG(dest_pt)                       AS dest_pt                       ,
         AVG(pos15000_access)               AS pos15000_access               ,
         AVG(pred_no2_2011_col_ppb)         AS pred_no2_2011_col_ppb         ,  
         AVG(sa1_prop_affordablehous_30_40) AS sa1_prop_affordablehous_30_40 ,
         AVG(sa2_prop_live_work_sa3)        AS sa2_prop_live_work_sa3        
         FROM {3}.clean_raw_ind_li_{1};
    '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_means_li_{0}', a summary of liveability indicator means.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_sd_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_sd_li_{1} AS
  SELECT stddev_pop(walkability)                  AS walkability                   ,
         stddev_pop(daily_living)                 AS daily_living                  ,
         stddev_pop(dd_nh1600m)                   AS dd_nh1600m                    ,
         stddev_pop(sc_nh1600m)                   AS sc_nh1600m                    ,
         stddev_pop(si_mix)                       AS si_mix                        ,
         stddev_pop(dest_pt)                      AS dest_pt                       ,
         stddev_pop(pos15000_access)              AS pos15000_access               ,
         stddev_pop(pred_no2_2011_col_ppb)        AS pred_no2_2011_col_ppb         ,
         stddev_pop(sa1_prop_affordablehous_30_40)AS sa1_prop_affordablehous_30_40 ,
         stddev_pop(sa2_prop_live_work_sa3)       AS sa2_prop_live_work_sa3        
         FROM {3}.clean_raw_ind_li_{1};       
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_sd_li_{0}', a summary of liveability indicator standard deviations.".format(i,uli_schema))
  
  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_min_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_min_li_{1} AS
  SELECT min(walkability)                  AS walkability                   ,
         min(daily_living)                 AS daily_living                  ,
         min(dd_nh1600m)                   AS dd_nh1600m                    ,
         min(sc_nh1600m)                   AS sc_nh1600m                    ,
         min(si_mix)                       AS si_mix                        ,
         min(dest_pt)                      AS dest_pt                       ,
         min(pos15000_access)              AS pos15000_access               ,
         min(pred_no2_2011_col_ppb)        AS pred_no2_2011_col_ppb         ,
         min(sa1_prop_affordablehous_30_40)AS sa1_prop_affordablehous_30_40 ,
         min(sa2_prop_live_work_sa3)       AS sa2_prop_live_work_sa3        
         FROM {3}.clean_raw_ind_li_{1};       
  '''.format(A_pointsID.lower(),i,exclusion_criteria,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_ind_summary_min_li_{0}'".format(i,uli_schema))

  createTable = '''
  DROP TABLE IF EXISTS {3}.clean_ind_summary_max_li_{1} ;        
  CREATE TABLE {3}.clean_ind_summary_max_li_{1} AS
  SELECT max(walkability)                  AS walkability                   ,
         max(daily_living)                 AS daily_living                  ,
         max(dd_nh1600m)                   AS dd_nh1600m                    ,
         max(sc_nh1600m)                   AS sc_nh1600m                    ,
         max(si_mix)                       AS si_mix                        ,
         max(dest_pt)                      AS dest_pt                       ,
         max(pos15000_access)              AS pos15000_access               ,
         max(pred_no2_2011_col_ppb)        AS pred_no2_2011_col_ppb         ,
         max(sa1_prop_affordablehous_30_40)AS sa1_prop_affordablehous_30_40 ,
         max(sa2_prop_live_work_sa3)       AS sa2_prop_live_work_sa3        
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
         100 + 10 * (t.walkability                  - _mean.walkability                   ) / _sd.walkability                  ::double precision AS walkability,
         100 + 10 * (t.daily_living                 - _mean.daily_living                  ) / _sd.daily_living                 ::double precision AS daily_living,
         100 + 10 * (t.dd_nh1600m                   - _mean.dd_nh1600m                    ) / _sd.dd_nh1600m                   ::double precision AS dd_nh1600m  ,
         100 + 10 * (t.sc_nh1600m                   - _mean.sc_nh1600m                    ) / _sd.sc_nh1600m                   ::double precision AS sc_nh1600m  ,
         100 + 10 * (t.si_mix                       - _mean.si_mix                        ) / _sd.si_mix                       ::double precision AS si_mix,
         100 + 10 * (t.dest_pt                      - _mean.dest_pt                       ) / _sd.dest_pt                      ::double precision AS dest_pt,
         100 + 10 * (t.pos15000_access              - _mean.pos15000_access               ) / _sd.pos15000_access              ::double precision AS pos15000_access,
         100 - 10 * (t.pred_no2_2011_col_ppb        - _mean.pred_no2_2011_col_ppb         ) / _sd.pred_no2_2011_col_ppb        ::double precision AS pred_no2_2011_col_ppb ,  
         100 + 10 * (t.sa1_prop_affordablehous_30_40- _mean.sa1_prop_affordablehous_30_40 ) / _sd.sa1_prop_affordablehous_30_40::double precision AS sa1_prop_affordablehousing,
         100 + 10 * (t.sa2_prop_live_work_sa3       - _mean.sa2_prop_live_work_sa3        ) / _sd.sa2_prop_live_work_sa3       ::double precision AS sa2_prop_live_work_sa3
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
               unnest(array[walkability,si_mix,dest_pt,pos15000_access,pred_no2_2011_col_ppb,sa1_prop_affordablehousing,sa2_prop_live_work_sa3]) as val 
        FROM {2}.clean_ind_mpi_norm_{1} ) alias
  GROUP BY {0};
  '''.format(A_pointsID.lower(),i,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_li_ci_{0}_est', a parcel level composite indicator estimate for liveability.".format(i,uli_schema))

  createTable = ''' 
  -- 2. Create MPI estimates at parcel level EXCLUDING AIR QUALITY (as basis for  sensitivity analysis)
  -- rowmean*(1-(rowsd(z_j)/rowmean(z_j))^2) AS mpi_est_j
  DROP TABLE IF EXISTS {2}.clean_li_ci_{1}_excl_airqual ; 
  CREATE TABLE {2}.clean_li_ci_{1}_excl_airqual AS
  SELECT {0}, AVG(val) AS mean, stddev_pop(val) AS sd, stddev_pop(val)/AVG(val) AS cv, AVG(val)-(stddev_pop(val)^2)/AVG(val) AS li_ci_excl_airqual 
  FROM (SELECT {0}, 
               unnest(array[walkability,si_mix,dest_pt,pos15000_access,sa1_prop_affordablehousing,sa2_prop_live_work_sa3]) as val 
        FROM {2}.clean_ind_mpi_norm_{1} ) alias
  GROUP BY {0};
  '''.format(A_pointsID.lower(),i,uli_schema)
  
  curs.execute(createTable)
  conn.commit()
  print("Created table '{1}.clean_li_ci_{0}_excl_airqual', a parcel level composite indicator estimate for liveability EXCLUDING AIR QUALITY (as basis for sensitivity analysis).".format(i,uli_schema))
  
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
         li_ci_excl_airqual,
         walkability,
         daily_living,
         dd_nh1600m  ,
         sc_nh1600m  ,
         si_mix,
         dest_pt,
         pos15000_access,
         pred_no2_2011_col_ppb,   
         sa1_prop_affordablehousing,
         sa2_prop_live_work_sa3
  FROM {2}.clean_ind_mpi_norm_{1}
  LEFT JOIN {2}.clean_li_ci_{1}_est  ON {2}.clean_li_ci_{1}_est.{0} = {2}.clean_ind_mpi_norm_{1}.{0}
  LEFT JOIN {2}.clean_li_ci_{1}_excl_airqual ON {2}.clean_li_ci_{1}_excl_airqual.{0} = {2}.clean_ind_mpi_norm_{1}.{0};
  ALTER TABLE {2}.clean_li_parcel_ci_{1} ADD PRIMARY KEY ({0});
  -- export for analysis, although this may be better achieved through sql queries by SA1
      -- same effect but lighter on memory
  -- COPY {2}.clean_li_parcel_ci_{1} TO 'C:/data/liveability/data/li_parcel_ci_{1}.csv' DELIMITER ',' CSV HEADER;
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
             li_ci_excl_airqual,
             walkability,
             daily_living,
             dd_nh1600m,
             sc_nh1600m,
             si_mix,
             dest_pt,
             pos_greq15000m2_in_400m_{1} AS pos15000_access,
             pred_no2_2011_col_ppb,     
             sa1_prop_affordablehous_30_40 AS sa1_prop_affordablehousing,
             sa2_prop_live_work_sa3
  FROM parcelmb 
  LEFT JOIN abs_linkage ON parcelmb.mb_code11 = abs_linkage.mb_code11
  LEFT JOIN non_abs_linkage ON parcelmb.{0} = non_abs_linkage.{0}
  LEFT JOIN {3}.clean_li_ci_{1}_est ON {3}.clean_li_ci_{1}_est.{0} = parcelmb.{0}
  LEFT JOIN {3}.clean_li_ci_{1}_excl_airqual ON {3}.clean_li_ci_{1}_excl_airqual.{0} = parcelmb.{0}
  LEFT JOIN ind_abs ON parcelmb.{0} = ind_abs.{0}
  LEFT JOIN ind_walkability_{1} ON parcelmb.{0} = ind_walkability_{1}.{0}
  LEFT JOIN ind_daily_living_{1} ON parcelmb.{0} = ind_daily_living_{1}.{0}
  LEFT JOIN dwelling_density ON parcelmb.{0} = dwelling_density.{0}
  LEFT JOIN street_connectivity ON parcelmb.{0} = street_connectivity.{0}
  LEFT JOIN ind_si_mix_{1} ON parcelmb.{0} = ind_si_mix_{1}.{0}
  LEFT JOIN ind_dest_pt_{1} ON parcelmb.{0} = ind_dest_pt_{1}.{0}
  LEFT JOIN ind_pos ON parcelmb.{0} = ind_pos.{0}
  LEFT JOIN no2_pred ON parcelmb.mb_code11 = no2_pred.mb_code11
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
         round(100*cume_dist() OVER(ORDER BY li_ci_excl_airqual)::numeric,0) as li_ci_excl_airqual,
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
      AVG(li_ci_est                   ) AS li_ci_est                   ,
      AVG(li_ci_excl_airqual          ) AS li_ci_excl_airqual          ,
      AVG(walkability                 ) AS walkability                 ,
      AVG(daily_living                ) AS daily_living                ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                  ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                  ,
      AVG(si_mix                      ) AS si_mix                      ,
      AVG(dest_pt                     ) AS dest_pt                     ,
      AVG(pos15000_access             ) AS pos15000_access             ,
      AVG(pred_no2_2011_col_ppb       ) AS pred_no2_2011_col_ppb       ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing  ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3  
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
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                   ,
      stddev_pop(li_ci_excl_airqual          ) AS sd_li_ci_excl_airqual          ,
      stddev_pop(walkability                 ) AS sd_walkability                 ,
      stddev_pop(daily_living                ) AS sd_daily_living                ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                  ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                  ,
      stddev_pop(si_mix                      ) AS sd_si_mix                      ,
      stddev_pop(dest_pt                     ) AS sd_dest_pt                     ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access             ,
      stddev_pop(pred_no2_2011_col_ppb       ) AS sd_pred_no2_2011_col_ppb       ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing  ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3      
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
      round(min(li_centile                  )::numeric,1)::text || ' - ' ||round(max(li_centile                  )::numeric,1)::text AS li_centile                 ,
      round(min(walkability                 )::numeric,1)::text || ' - ' ||round(max(walkability                 )::numeric,1)::text AS walkability                 ,
      round(min(daily_living                )::numeric,1)::text || ' - ' ||round(max(daily_living                )::numeric,1)::text AS daily_living                ,
      round(min(dd_nh1600m                  )::numeric,1)::text || ' - ' ||round(max(dd_nh1600m                  )::numeric,1)::text AS dd_nh1600m                  ,
      round(min(sc_nh1600m                  )::numeric,1)::text || ' - ' ||round(max(sc_nh1600m                  )::numeric,1)::text AS sc_nh1600m                  ,
      round(min(si_mix                      )::numeric,1)::text || ' - ' ||round(max(si_mix                      )::numeric,1)::text AS si_mix                      ,
      round(min(100*dest_pt                 )::numeric,1)::text || ' - ' ||round(max(100*dest_pt                 )::numeric,1)::text AS dest_pt                     ,
      round(min(100*pos15000_access         )::numeric,1)::text || ' - ' ||round(max(100*pos15000_access         )::numeric,1)::text AS pos15000_access             ,
      round(min(pred_no2_2011_col_ppb       )::numeric,1)::text || ' - ' ||round(max(pred_no2_2011_col_ppb       )::numeric,1)::text AS pred_no2_2011_col_ppb       ,
      round(min(100*sa1_prop_affordablehousing  )::numeric,1)::text || ' - ' ||round(max(100*sa1_prop_affordablehousing  )::numeric,1)::text AS sa1_prop_affordablehousing  ,
      round(min(100*sa2_prop_live_work_sa3  )::numeric,1)::text || ' - ' ||round(max(100*sa2_prop_live_work_sa3      )::numeric,1)::text AS sa2_prop_live_work_sa3,  
      round(min(li_excl_airq_centile        )::numeric,1)::text || ' - ' ||round(max(li_excl_airq_centile       )::numeric,1)::text AS li_excl_airq_centile       
      FROM {2}.raw_indicators_{1}  AS t1
      LEFT JOIN
      (SELECT detail_pid, 
              100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile,
              100*cume_dist() OVER(ORDER BY li_ci_excl_airqual)::numeric AS li_excl_airq_centile 
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
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY walkability                     )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY walkability                 )::numeric,1)::text AS walkability                 ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY daily_living                    )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY daily_living                )::numeric,1)::text AS daily_living                ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY dd_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY dd_nh1600m                  )::numeric,1)::text AS dd_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY sc_nh1600m                      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY sc_nh1600m                  )::numeric,1)::text AS sc_nh1600m                  ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY si_mix                          )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY si_mix                      )::numeric,1)::text AS si_mix                      ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*dest_pt                     )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*dest_pt                 )::numeric,1)::text AS dest_pt                     ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*pos15000_access             )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*pos15000_access         )::numeric,1)::text AS pos15000_access             ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY pred_no2_2011_col_ppb           )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY pred_no2_2011_col_ppb       )::numeric,1)::text AS pred_no2_2011_col_ppb       ,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa1_prop_affordablehousing  )::numeric,1)::text AS sa1_prop_affordablehousing,
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3      )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY 100*sa2_prop_live_work_sa3  )::numeric,1)::text AS sa2_prop_live_work_sa3,  
      round(percentile_cont(0.1) WITHIN GROUP (ORDER BY li_excl_airq_centile            )::numeric,1)::text || ' - ' ||round(percentile_cont(0.9) WITHIN GROUP (ORDER BY li_excl_airq_centile        )::numeric,1)::text AS li_excl_airq_centile                   
      FROM {2}.raw_indicators_{1} AS t1
      LEFT JOIN 
      (SELECT detail_pid, 
              100*cume_dist() OVER(ORDER BY li_ci_est)::numeric AS li_centile,
              100*cume_dist() OVER(ORDER BY li_ci_excl_airqual)::numeric AS li_excl_airq_centile 
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
      AVG(li_ci_est                   ) AS li_ci_est                   ,
      AVG(walkability                 ) AS walkability                 ,
      AVG(daily_living                ) AS daily_living                ,
      AVG(dd_nh1600m                  ) AS dd_nh1600m                  ,
      AVG(sc_nh1600m                  ) AS sc_nh1600m                  ,
      AVG(si_mix                      ) AS si_mix                      ,
      AVG(dest_pt                     ) AS dest_pt                     ,
      AVG(pos15000_access             ) AS pos15000_access             ,
      AVG(pred_no2_2011_col_ppb       ) AS pred_no2_2011_col_ppb       ,
      AVG(sa1_prop_affordablehousing  ) AS sa1_prop_affordablehousing  ,
      AVG(sa2_prop_live_work_sa3      ) AS sa2_prop_live_work_sa3      ,
      AVG(li_ci_excl_airqual          ) AS li_ci_excl_airqual                   
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
      stddev_pop(li_ci_est                   ) AS sd_li_ci_est                   ,
      stddev_pop(walkability                 ) AS sd_walkability                 ,
      stddev_pop(daily_living                ) AS sd_daily_living                ,
      stddev_pop(dd_nh1600m                  ) AS sd_dd_nh1600m                  ,
      stddev_pop(sc_nh1600m                  ) AS sd_sc_nh1600m                  ,
      stddev_pop(si_mix                      ) AS sd_si_mix                      ,
      stddev_pop(dest_pt                     ) AS sd_dest_pt                     ,
      stddev_pop(pos15000_access             ) AS sd_pos15000_access             ,
      stddev_pop(pred_no2_2011_col_ppb       ) AS sd_pred_no2_2011_col_ppb       ,
      stddev_pop(sa1_prop_affordablehousing  ) AS sd_sa1_prop_affordablehousing  ,
      stddev_pop(sa2_prop_live_work_sa3      ) AS sd_sa2_prop_live_work_sa3      ,
      stddev_pop(li_ci_excl_airqual          ) AS sd_li_ci_excl_airqual               
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
           round(10*cume_dist() OVER(ORDER BY walkability                 )::numeric,0) as walkability                 ,
           round(10*cume_dist() OVER(ORDER BY daily_living                )::numeric,0) as daily_living                ,
           round(10*cume_dist() OVER(ORDER BY dd_nh1600m                  )::numeric,0) as dd_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY sc_nh1600m                  )::numeric,0) as sc_nh1600m                  ,
           round(10*cume_dist() OVER(ORDER BY si_mix                      )::numeric,0) as si_mix                      ,
           round(10*cume_dist() OVER(ORDER BY dest_pt                     )::numeric,0) as dest_pt                     ,
           round(10*cume_dist() OVER(ORDER BY pos15000_access             )::numeric,0) as pos15000_access             ,
           round(10*cume_dist() OVER(ORDER BY pred_no2_2011_col_ppb       )::numeric,0) as pred_no2_2011_col_ppb       ,
           round(10*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing  )::numeric,0) as sa1_prop_affordablehousing  ,
           round(10*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3      )::numeric,0) as sa2_prop_live_work_sa3      ,
           round(10*cume_dist() OVER(ORDER BY li_ci_excl_airqual          )::numeric,0) as li_ci_excl_airqual                   
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
           round(100*cume_dist() OVER(ORDER BY li_ci_est                   )::numeric,0) as li_ci_est                   ,
           round(100*cume_dist() OVER(ORDER BY walkability                 )::numeric,0) as walkability                 ,
           round(100*cume_dist() OVER(ORDER BY daily_living                )::numeric,0) as daily_living                ,
           round(100*cume_dist() OVER(ORDER BY dd_nh1600m                  )::numeric,0) as dd_nh1600m                  ,
           round(100*cume_dist() OVER(ORDER BY sc_nh1600m                  )::numeric,0) as sc_nh1600m                  ,
           round(100*cume_dist() OVER(ORDER BY si_mix                      )::numeric,0) as si_mix                      ,
           round(100*cume_dist() OVER(ORDER BY dest_pt                     )::numeric,0) as dest_pt                     ,
           round(100*cume_dist() OVER(ORDER BY pos15000_access             )::numeric,0) as pos15000_access             ,
           round(100*cume_dist() OVER(ORDER BY pred_no2_2011_col_ppb       )::numeric,0) as pred_no2_2011_col_ppb       ,
           round(100*cume_dist() OVER(ORDER BY sa1_prop_affordablehousing  )::numeric,0) as sa1_prop_affordablehousing  ,
           round(100*cume_dist() OVER(ORDER BY sa2_prop_live_work_sa3      )::numeric,0) as sa2_prop_live_work_sa3      ,
           round(100*cume_dist() OVER(ORDER BY li_ci_excl_airqual          )::numeric,0) as li_ci_excl_airqual                
    FROM {2}.clean_li_mpi_norm_{1}_{0} 
    ORDER BY {0} ASC;
    ALTER TABLE {2}.clean_li_percentiles_{1}_{0} ADD PRIMARY KEY ({0});
    '''.format(area,type,uli_schema)

    curs.execute(createTable)
    conn.commit()
    print("Created {1} percentiles at {0} level for schema {2}".format(area,type,uli_schema))  
  
# Create shape files for interactive map visualisation
areas = ['sa1_7dig11','ssc_name','lga_name11']
short_area = ['sa1','ssc','lga']
area_strings = ['''t1.sa1_7dig11                                   AS f1 ,
                  suburb                                          AS f2 ,
                  lga                                             AS f3 ,''',
               ''' '-'::varchar                                    AS f1 ,
                   t1.ssc_name                                     AS f2 ,
                   lga                                             AS f3 ,''',
               ''' '-'::varchar                                    AS f1 ,
                   '-'::varchar                                    AS f2 ,
                   t1.lga_name11                                   AS f3 ,''']  
geom_tables = ['''LEFT JOIN sa1_2011_AUST AS t5 ON t1.sa1_7dig11 = t5.sa1_7dig11::numeric''',
               '''LEFT JOIN ssc_2011_AUST AS t5 ON t1.ssc_name = t5.ssc_name''',
               '''LEFT JOIN abs.lga_2011_AUST AS t5 ON t1.lga_name11 = t5.lga_name11''']
area_tables = ['''LEFT JOIN {0}.sa1_area AS t6 ON t1.sa1_7dig11 = t6.sa1_7dig11'''.format(uli_schema),
               '''LEFT JOIN {0}.ssc_area AS t6 ON t1.ssc_name = t6.suburb'''.format(uli_schema),
               '']
community_code = ['''LEFT JOIN (SELECT sa1_7dig11, sa1_7dig11::varchar AS community_code FROM sa1_2011_AUST) AS t7 ON t1.sa1_7dig11 = t7.sa1_7dig11::numeric''',
                  '''LEFT JOIN (SELECT ssc_name, CONCAT('SSC',ssc_code::varchar) AS community_code FROM ssc_2011_AUST) AS t7 ON t1.ssc_name = t7.ssc_name''',
                  '''LEFT JOIN (SELECT lga_name11, CONCAT('LGA',lga_code11::varchar) AS community_code FROM abs.lga_2011_AUST) AS t7 ON t1.lga_name11 = t7.lga_name11''']

for area in areas:   
    createTable = '''DROP TABLE IF EXISTS {6}.clean_li_map_{0};
    CREATE TABLE {6}.clean_li_map_{0} AS
    SELECT {1}
    round(t1.walkability::numeric,1) AS rh1,
    round(t1.daily_living::numeric,1) AS rh2,
    round(t1.dd_nh1600m::numeric,1) AS rh3,
    round(t1.sc_nh1600m::numeric,1) AS rh4,
    round(t1.si_mix::numeric,1) AS rh5,
    round(100*t1.dest_pt::numeric,1) AS rh6,
    round(100*t1.pos15000_access::numeric,1) AS rh7,
    round(100*t1.sa1_prop_affordablehousing::numeric,1) AS rh8,
    round(100*t1.sa2_prop_live_work_sa3::numeric,1) AS rh9,
    round(t1.pred_no2_2011_col_ppb::numeric,1) AS rh10,
    round(t2.li_ci_est::numeric,0) AS ph0,
    round(t2.walkability::numeric,0) AS ph1,
    round(t2.daily_living::numeric,0) AS ph2,
    round(t2.dd_nh1600m::numeric,0) AS ph3,
    round(t2.sc_nh1600m::numeric,0) AS ph4,
    round(t2.si_mix::numeric,0) AS ph5,
    round(t2.dest_pt::numeric,0) AS ph6,
    round(t2.pos15000_access::numeric,0) AS ph7,
    round(t2.sa1_prop_affordablehousing::numeric,0) AS ph8,
    round(t2.sa2_prop_live_work_sa3::numeric,0) AS ph9,
    round(t2.pred_no2_2011_col_ppb::numeric,0) AS ph10,
    trh.li_centile AS dh0,
    trh.walkability AS dh1,
    trh.daily_living AS dh2,
    trh.dd_nh1600m AS dh3,
    trh.sc_nh1600m AS dh4,
    trh.si_mix AS dh5,
    trh.dest_pt AS dh6,
    trh.pos15000_access AS dh7,
    trh.sa1_prop_affordablehousing AS dh8,
    trh.sa2_prop_live_work_sa3 AS dh9,
    trh.pred_no2_2011_col_ppb AS dh10,
    tmh.li_centile AS mh0,
    tmh.walkability AS mh1,
    tmh.daily_living AS mh2,
    tmh.dd_nh1600m AS mh3,
    tmh.sc_nh1600m AS mh4,
    tmh.si_mix AS mh5,
    tmh.dest_pt AS mh6,
    tmh.pos15000_access AS mh7,
    tmh.sa1_prop_affordablehousing AS mh8,
    tmh.sa2_prop_live_work_sa3 AS mh9,
    tmh.pred_no2_2011_col_ppb AS mh10,
    round(t3.walkability::numeric,1) AS rs1,
    round(t3.daily_living::numeric,1) AS rs2,
    round(t3.si_mix::numeric,1) AS rs5,
    round(100*t3.dest_pt::numeric,1) AS rs6,
    round(100*t3.pos15000_access::numeric,1) AS rs7,
    round(t4.li_ci_est::numeric,0) AS ps0,
    round(t4.walkability::numeric,0) AS ps1,
    round(t4.daily_living::numeric,0) AS ps2,
    round(t4.si_mix::numeric,0) AS ps5,
    round(t4.dest_pt::numeric,0) AS ps6,
    round(t4.pos15000_access::numeric,0) AS ps7,
    trs.li_centile AS ds0,
    trs.walkability AS ds1,
    trs.daily_living AS ds2,
    trs.si_mix AS ds5,
    trs.dest_pt AS ds6,
    trs.pos15000_access AS ds7,
    tms.li_centile AS ms0,
    tms.walkability AS ms1,
    tms.daily_living AS ms2,
    tms.si_mix AS ms5,
    tms.dest_pt AS ms6,
    tms.pos15000_access AS ms7,
    round(t2.li_ci_excl_airqual::numeric,0) AS ph11,
    trh.li_excl_airq_centile AS dh11,    
    tmh.li_excl_airq_centile AS mh11,
    round(t4.li_ci_excl_airqual::numeric,0) AS ps11,
    trs.li_excl_airq_centile AS ds11,    
    tms.li_excl_airq_centile AS ms11,
    gid,        
    community_code,
    ST_TRANSFORM(geom,4326) AS geom              
    FROM {6}.li_raw_hard_{2} AS t1 
    LEFT JOIN {6}.clean_li_percentiles_hard_{2} AS t2 ON t1.{2}  = t2.{2}
    LEFT JOIN {6}.li_range_hard_{2} AS trh ON t1.{2}  = trh.{2}
    LEFT JOIN {6}.li_most_hard_{2}  AS tmh ON t1.{2}  = tmh.{2}
    LEFT JOIN {6}.li_raw_soft_{2} AS t3  ON t1.{2}  = t3.{2}
    LEFT JOIN {6}.clean_li_percentiles_soft_{2} AS t4  ON t1.{2}  = t4.{2}
    LEFT JOIN {6}.li_range_soft_{2} AS trs ON t1.{2}  = trs.{2}
    LEFT JOIN {6}.li_most_soft_{2}  AS tms ON t1.{2}  = tms.{2}
    {3}
    {4}
    {5};'''.format(short_area[areas.index(area)],area_strings[areas.index(area)],area,geom_tables[areas.index(area)],area_tables[areas.index(area)],community_code[areas.index(area)],uli_schema)
    print(createTable)
    curs.execute(createTable)
    conn.commit()
    command = 'pgsql2shp -f {0}clean_li_map_{1}.shp -h {2} -u {3} -P {4} {5} {6}.clean_li_map_{1}'.format(outpath,short_area[areas.index(area)],sqlDBHost,sqlUserName,sqlPWD,sqlDBName,uli_schema)
    sp.call(command.split())

print("--Created SA1, suburb and LGA level tables for map web app for schema {0}".format(uli_schema))      
conn.close()


# output to completion log    
script_running_log(script, task, start)
 