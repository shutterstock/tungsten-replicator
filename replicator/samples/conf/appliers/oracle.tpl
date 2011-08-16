replicator.applier.oracle.service=@{APPLIER.REPL_ORACLE_SERVICE}
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.OracleApplier
replicator.applier.dbms.host=${replicator.global.db.host}
replicator.applier.dbms.port=${replicator.global.db.port}
replicator.applier.dbms.user=${replicator.global.db.user}
replicator.applier.dbms.password=${replicator.global.db.password}
replicator.applier.dbms.service=${replicator.applier.oracle.service}
#replicator.applier.dbms.maxSQLLogLength=3000