# Shard filter.  Looks for and enforces shard master locations. 
replicator.filter.shardfilter=com.continuent.tungsten.replicator.shard.ShardFilter

# Enable the shard filter.  You can set this to false to disable operation. 
replicator.filter.shardfilter.enabled=true

# Policy for handling unknown shards.  Legal values are accept, drop, warn,
# and error.
replicator.filter.shardfilter.unknownShardPolicy=error

# Decide whether to enforce homes.  If unset, we allow all shards. 
replicator.filter.shardfilter.enforceHome=false

# Decide whether to permit shard rules to be created automatically.
replicator.filter.shardfilter.autoCreate=false

