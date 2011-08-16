# Database transform filter.  Transforms database names that match the 
# from_regex are transformed into the to_regex.  
replicator.filter.dbtransform=com.continuent.tungsten.replicator.filter.DatabaseTransformFilter
replicator.filter.dbtransform.from_regex=foo
replicator.filter.dbtransform.to_regex=bar