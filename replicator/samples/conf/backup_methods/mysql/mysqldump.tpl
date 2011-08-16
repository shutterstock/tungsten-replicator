# MySqlDump Agent--backup using mysql dump utility; restore with mysql.
replicator.backup.agent.mysqldump=com.continuent.tungsten.replicator.backup.mysql.MySqlDumpAgent
replicator.backup.agent.mysqldump.host=${replicator.global.db.host}
replicator.backup.agent.mysqldump.port=${replicator.global.db.port}
replicator.backup.agent.mysqldump.user=${replicator.global.db.user}
replicator.backup.agent.mysqldump.password=${replicator.global.db.password}
replicator.backup.agent.mysqldump.dumpDir=@{REPL_BACKUP_DUMP_DIR}
replicator.backup.agent.mysqldump.mysqldumpOptions=--opt --all-databases --add-drop-database
replicator.backup.agent.mysqldump.hotBackupEnabled=true