# Load script for MySQL.  First command ensures timezone is set to GMT. 
# Note the use of UTF8 charset.  MySQL and the replicator must likewise
# be configured to use UTF8. 
SET time_zone = '+0:00';
LOAD DATA LOCAL INFILE '%%CSV_FILE%%' INTO TABLE %%STAGE_TABLE%% 
  CHARACTER SET utf8 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
