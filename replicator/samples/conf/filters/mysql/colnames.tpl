# Column name filter.  Adds column name metadata to row updates.  This is 
# required for MySQL row replication if you have logic that requires column
# names. 
replicator.filter.colnames=com.continuent.tungsten.replicator.filter.ColumnNameFilter