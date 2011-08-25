# Shard filter.  Looks for and enforces shard master locations. 
replicator.filter.shardfilter=com.continuent.tungsten.replicator.shard.ShardFilter

# Decide whether to enforce homes.  If unset, this filter does nothing.
replicator.filter.shardfilter.enforceHome=false

# Decide whether to permit shard rules to be created automatically.
replicator.filter.shardfilter.autoCreate=false

# Policy for handling unknown shards.  Legal values are accept, drop, warn,
# and error.
replicator.filter.shardfilter.unknownMasterPolicy=error

