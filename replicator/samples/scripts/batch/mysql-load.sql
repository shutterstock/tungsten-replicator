# Load script for MySQL.  First command ensures timezone is set to GMT. 
# Note the use of UTF8 charset.  MySQL and the replicator must likewise
# be configured to use UTF8.  This script *must* run on the server.  Tungsten
# uses drizzle JDBC which does not handle LOAD DATA LOCAL INFILE properly. 
SET time_zone = '+0:00';
LOAD DATA INFILE '%%CSV_FILE%%' INTO TABLE %%STAGE_TABLE%% 
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
