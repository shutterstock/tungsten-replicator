# Batch applier basic configuration information. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.batch.BatchApplier
replicator.applier.dbms.url=@{APPLIER.REPL_DBTHLURL}
replicator.applier.dbms.driver=@{APPLIER.REPL_DBJDBCDRIVER}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}

# Timezone and character set. 
replicator.applier.dbms.timezone=GMT+0:00
replicator.applier.dbms.charset=UTF-8

# Load method, COPY command template, and staging directory location. 
replicator.applier.dbms.loadMethod=direct
replicator.applier.dbms.loadBatchTemplate=@{SERVICE.BATCH_LOAD_TEMPLATE}
replicator.applier.dbms.stageDirectory=/tmp/staging
replicator.applier.dbms.supportsReplace=true

# Extra parameters for loading via stage tables. 
replicator.applier.dbms.stageTablePrefix=stage_xxx
replicator.applier.dbms.stageInsertFromTemplate=@{SERVICE.BATCH_INSERT_TEMPLATE}
replicator.applier.dbms.stageDeleteFromTemplate=@{SERVICE.BATCH_DELETE_TEMPLATE}
replicator.applier.dbms.stagePkeyColumn=id
replicator.applier.dbms.stageRowIdColumn=row_id
replicator.applier.dbms.cleanUpFiles=false