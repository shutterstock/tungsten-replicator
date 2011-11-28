replicator.filter.prefetch=com.continuent.tungsten.replicator.filter.PrefetchFilter
replicator.filter.prefetch.url=@{APPLIER.REPL_DBBASICJDBCURL}
replicator.filter.prefetch.aheadMaxTime=@{PREFETCH_TIME_AHEAD}
replicator.filter.prefetch.sleepTime=@{PREFETCH_SLEEP_TIME}
replicator.filter.prefetch.warmUpEventCount=@{PREFETCH_WARMUP_EVENT_COUNT}