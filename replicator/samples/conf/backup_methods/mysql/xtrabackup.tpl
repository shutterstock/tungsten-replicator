# Xtrabackup Agent--Executes a script that uses xtrabackup to backup or restore. 
replicator.backup.agent.xtrabackup=com.continuent.tungsten.replicator.backup.generic.ScriptDumpAgent
replicator.backup.agent.xtrabackup.script=${replicator.home.dir}/samples/scripts/backup/xtrabackup.sh
replicator.backup.agent.xtrabackup.commandPrefix=@{REPL_BACKUP_COMMAND_PREFIX}
replicator.backup.agent.xtrabackup.hotBackupEnabled=true
# Xtrabackup can handle the following options
#   user            - The mysql user to use during backup [default: root]
#   password        - The password for the mysql user [default: ]
#		host						- The hostname for the database server [default: localhost]
#		port						- The port for the database server [default: 3306]
#   directory       - A working directory to stage backup files in [default: /tmp/innobackup]
#   archive         - A non-existing file that will be created to package the backup files [default: /tmp/innobackup.tar]
#   service         - The name of the mysql service [default: mysql]
#   mysqldatadir    - The absolute path for the mysql data directory [default: /var/lib/mysql]
#   mysqluser       - The os user that mysql runs as [default: mysql]
#   mysqlgroup      - The os group that mysql runs as [default: mysql]
#		mysql_service_comand	- The command to call when stopping/starting MySQL [default: /etc/init.d/mysql]
# replicator.backup.agent.xtrabackup.options=user=tungsten&password=secret&directory=/tmp/backup
replicator.backup.agent.xtrabackup.options=user=${replicator.global.db.user}&password=${replicator.global.db.password}&host=${replicator.global.db.host}&port=${replicator.global.db.port}&directory=@{REPL_MYSQL_XTRABACKUP_DIR}&archive=@{REPL_MYSQL_XTRABACKUP_FILE}&mysqldatadir=@{APPLIER.REPL_MYSQL_DATADIR}&mysql_service_command=@{APPLIER.REPL_BOOT_SCRIPT}