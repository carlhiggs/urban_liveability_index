# Purpose: summarise within and between area variation
# Author:  Carl Higgs 
# Date:    20170418

import os,sys
import time
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from collections import OrderedDict

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Summarise variables by area, with report on within and between area variation'

folderPath = parser.get('data', 'folderPath')

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

A_pointsID = parser.get('parcels', 'parcel_id')

def varvar(database = sqlDBName, user = sqlUserName, pw = sqlPWD, linkage_id = A_pointsID,var = '__none__', var_table = '__none__',area_code = '__none__',copy = '__none__', suffix = '', plot='none', varlab = 'none', subset='none',subset_linkage='none', subset_hist='none', header = 'false', col_length = 52, histmax = 0.6): 
  parameters = 'database', 'user', 'pw', 'var', 'var_table','area_code','linkage_id'
  res_length = max(len(area_code),7)
  if header=='true':
    
    print("{} {} {} {}\n{} {} {} {}\n{} {} {} {}\n{} {} {} {}\n".format("".ljust(col_length),"within".center(res_length),"between".center(res_length),"ratio".center(res_length),"".ljust(col_length),area_code.center(res_length),area_code.center(res_length),"avg(sd_w)".center(res_length),"".ljust(col_length),"sd".center(res_length),"sd".center(res_length),"/".center(res_length),"".ljust(col_length),"(mean)".center(res_length),"".center(res_length),"sd_b".center(res_length)))
  else:
    i = 0
    u = 0
    for item in (database, user, pw, var, var_table,area_code,linkage_id):
      if item is '__none__':
        print("{} is undefined, but should be!".format(parameters[i]))
        u+=1
      i+=1
      
    if u > 0:
      print("Re-run with fully defined parameter set: {}".format(parameters))
      print("Optionally, you can also specify a copy = '' argument containing a directory string to save a csv file of table.")
      return
        
    clause = ''
    subset_true = ''
    if subset != 'none':
      area_subset = pd.read_csv(subset, usecols=[0])
      areas = area_subset.ix[:, 0].tolist()
      clause = "where {} in ({})".format(subset_linkage,str(areas)[1:-1])
      subset_true = "subset"
    
    createTable = '''
    -- summary statistics
    DROP TABLE IF EXISTS temp ; 
    CREATE TABLE temp AS
    SELECT {0},
         COUNT({1})        AS n_parcels,
         AVG({1})          AS m_{1},                                    
         stddev_pop({1})/sqrt(COUNT({1}))  as se_{1},
         AVG({1}) - 1.96* stddev_pop({1})/sqrt(COUNT({1})) AS ll_ci,
         AVG({1}) + 1.96* stddev_pop({1})/sqrt(COUNT({1})) AS ul_ci,
         percentile_cont(.5) WITHIN GROUP (ORDER BY {1}) AS p_med_50, 
         percentile_cont(.025) WITHIN GROUP (ORDER BY {1}) AS p_ll_025, 
         percentile_cont(.975) WITHIN GROUP (ORDER BY {1}) AS p_ul_975,
         stddev_pop({1})  as within_{0}_sd
    FROM {2} 
    GROUP BY {0} 
    ORDER BY {0} ASC;
    
    -- variation summary
    DROP TABLE IF EXISTS {0}_summary_{1}_{5} ; 
    CREATE TABLE {0}_summary_{1}_{5} AS
    SELECT temp.{0},
           n_parcels,
           m_{1},
           se_{1},
           ll_ci,
           ul_ci,
           p_med_50,
           p_ll_025,
           p_ul_975,
           within_{0}_sd,
           avg_within_{0}_sd,
           between_{0}_sd,
           within_{0}_sd/between_{0}_sd AS ratio_wi_bw_var,
           avg_within_{0}_sd / between_{0}_sd AS ratio_avg_wi_bw_var
    FROM temp
    CROSS JOIN (SELECT AVG(within_{0}_sd) AS avg_within_{0}_sd  FROM temp) AS t2
    CROSS JOIN (SELECT stddev_pop(m_{1}) AS between_{0}_sd FROM temp) AS t3;
    
    -- clean up
    DROP TABLE temp;
    '''.format(area_code, var, var_table,linkage_id,clause,suffix)
     
    conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    curs.execute(createTable)
    
    filename = os.path.join(copy,"{}_{}_{}_{}.csv".format(subset_true,area_code,var,suffix))
    if copy is not 'none':
      if not os.path.exists(copy):
         os.makedirs(copy)
      outputquery = "COPY {0}_summary_{1}_{2} TO STDOUT DELIMITER ',' CSV HEADER;".format(area_code, var, suffix)
      with open(filename, 'w') as f:
        curs.copy_expert(outputquery, f)
        
    data = pd.read_csv(filename, quoting=2)
    summary = pd.DataFrame({str(varlab): data.loc[1,['avg_within_{}_sd'.format(area_code),'between_{}_sd'.format(area_code),'ratio_avg_wi_bw_var']].astype(np.double).round(2)}).T
    outpath = os.path.join(copy,'variation_summary_{}.csv'.format(area_code))
    
    if not os.path.isfile(outpath):
      summary.to_csv(outpath,header ='column_names',index=True, sep=',')
      print("{} {} {} {}".format(varlab.ljust(col_length),
                           str(data.iloc[1,10].astype(np.double).round(2)).center(res_length),
                           str(data.iloc[1,11].astype(np.double).round(2)).center(res_length),
                           str(data.iloc[1,13].astype(np.double).round(2)).center(res_length)))
    else:
      summary.to_csv(outpath, mode='a', index=True, sep=',', header=False)  
      print("{} {} {} {}".format(varlab.ljust(col_length),
                           str(data.iloc[1,10].astype(np.double).round(2)).center(res_length),
                           str(data.iloc[1,11].astype(np.double).round(2)).center(res_length),
                           str(data.iloc[1,13].astype(np.double).round(2)).center(res_length))) 
    
    
    if plot == 'true':
      if varlab == 'none':
        varlab = var

      # histogram of area average values
      weights = np.ones_like(data['m_{}'.format(var)])/float(len(data['m_{}'.format(var)]))
      plt.hist(data['m_{}'.format(var)], weights=weights, bins = 20)
      plt.title("{} ({})".format(varlab,suffix))
      plt.xlabel("Histogram of {} average {}".format(area_code,varlab))
      plt.ylabel("Proportion")
      plt.ylim(ymax=histmax)
      plt.savefig(os.path.join(copy,'hist_{0}_{1}_{2}_{3}.png'.format(subset_true,area_code,var,suffix)))
      plt.clf()  
        
      # histogram of ratio of within to between area variation
      weights = np.ones_like(data["ratio_wi_bw_var"])/float(len(data["ratio_wi_bw_var"]))
      plt.hist(data["ratio_wi_bw_var"], weights=weights, bins = 20)
      plt.title("{} ({})".format(varlab,suffix))
      plt.xlabel("ratio of within- to between- {0} variation".format(area_code))
      plt.ylabel("Proportion")
      plt.ylim(ymax=histmax)
      plt.savefig(os.path.join(copy,'w_b_variation_ratio_hist_{0}_{1}_{2}_{3}.png'.format(subset_true,area_code,var,suffix)))
      plt.clf()  
      
      # scatterplot of SE of area mean estimate by area mean, with size by ratio of within/between area variation 
      plt.scatter(data.loc[:,'m_{}'.format(var)],data.loc[:,'se_{}'.format(var)],s=10*data.loc[:,'ratio_wi_bw_var'])
      plt.title("{} ({})".format(varlab,suffix))
      plt.xlabel("{0}-level mean".format(area_code),verticalalignment='top')
      plt.ylabel("{0}-level standard error".format(area_code))
      plt.ylim(ymin=0)
      plt.figtext(0.01, 0.01, 'note: size by within-/between- area variation', horizontalalignment='left', size = 'x-small')
      plt.savefig(os.path.join(copy,'scatterplot_{0}_{1}_{2}_{3}.png'.format(subset_true,area_code,var,suffix)))
      plt.clf()  
       
      plt.close('all')
      conn.close()


print(task)
# loop over ind_type
for ind_type in ['li_parcel_ci','clean_li_parcel_ci','raw_indicators']:
  # loop over areas 
  # area within which to summarise variable (must exist as linkage in source table, specified below)
  for area_code in ['mb_code11','sa1_7dig11','sa3_name11','ssc_name','lga_name11']:
    # loop over liveability index indicators method
    for i in ['hard','soft']:
      print("\n{}: {}, {}".format(area_code,ind_type,i))
      var_table = '{0}_{1}'.format(ind_type,i)
      out_folder = os.path.join(folderPath,'../li_analysis/area_variation/{0}_{1}/{2}'.format(ind_type,i,area_code))
      output_plots = 'true'
      vars = OrderedDict([('li_ci_est','Liveability CI estimate'),
                          ('walkability','Walkability Index'),
                          ('daily_living','Daily Living score'),
                          ('dd_nh1600m','Dwelling density'),
                          ('sc_nh1600m','Street connectivity'),
                          ('si_mix','Social infrastructure mix score'),
                          ('dest_pt','Public transport access indicator'),
                          ('pos15000_access','POS >= 1.5Ha access indicator'),
                          ('pred_no2_2011_col_ppb','Air quality (rev. meshblock pred. NO_2)'),
                          ('sa1_prop_affordablehousing','Affordable housing in SA1'),
                          ('sa2_prop_live_work_sa3','SA2 Workers live & work in same SA3')])
      col_length = max([len(var) for var in vars.values()])        
      varvar(area_code = area_code, 
             header = 'true',
             col_length = col_length)            
      for var, varlab in vars.items():
        varvar(area_code = area_code, 
               var = var, 
               var_table = var_table, 
               copy = out_folder, 
               plot = output_plots, 
               varlab = varlab,
               suffix = i,
               col_length = col_length)
             
# output to completion log    
script_running_log(script, task, start)

