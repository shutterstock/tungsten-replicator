# Batch applier basic configuration information. 
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.batch.SimpleBatchApplier
replicator.applier.dbms.url=@{APPLIER.REPL_DBTHLURL}
replicator.applier.dbms.driver=@{APPLIER.REPL_DBJDBCDRIVER}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
replicator.applier.dbms.startupScript=${replicator.home.dir}/samples/scripts/batch/@{SERVICE.BATCH_LOAD_TEMPLATE}-connect.sql

# Timezone and character set. 
replicator.applier.dbms.timezone=GMT+0:00
replicator.applier.dbms.charset=UTF-8

# Parameters for loading and merging via stage tables. 
replicator.applier.dbms.stageColumnPrefix=tungsten_
replicator.applier.dbms.stageTablePrefix=stage_xxx_
replicator.applier.dbms.stageDirectory=/tmp/staging
replicator.applier.dbms.stageLoadScript=${replicator.home.dir}/samples/scripts/batch/@{SERVICE.BATCH_LOAD_TEMPLATE}-load.sql
replicator.applier.dbms.stageMergeScript=${replicator.home.dir}/samples/scripts/batch/@{SERVICE.BATCH_LOAD_TEMPLATE}-merge.sql
replicator.applier.dbms.cleanUpFiles=false

# Included to provide default pkey for tables that omit such.  This is not 
# a good practice in general. 
#replicator.applier.dbms.stagePkeyColumn=id
