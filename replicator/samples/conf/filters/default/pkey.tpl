# Primary key filter.  This filter is required for MySQL row replication to 
# reduce the number of columns used in comparisons for updates and deletes. 
replicator.filter.pkey=com.continuent.tungsten.replicator.filter.PrimaryKeyFilter
replicator.filter.pkey.url=@{APPLIER.REPL_DBTHLURL}