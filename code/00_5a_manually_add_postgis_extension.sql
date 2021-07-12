-- manually run this query in the project postgresql database that has been created

SELECT postgis_full_version();

-- This verifies whether the postgis extension has been successfully registered yet
-- If not, run the following command:

CREATE EXTENSION postgis;

-- Then, verify again:

SELECT postgis_full_version();

-- Carl Higgs, 22/03/2017