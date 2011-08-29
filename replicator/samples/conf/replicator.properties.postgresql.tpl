#####################################
# REPLICATOR.PROPERTIES.POSTGRESQL  #
#####################################
#
# This file contains properties for external replication through the 
# Open Replicator script plugin.
#
# NOTE TO WINDOWS USERS:  Single backslash characters are treated as escape 
# characters.  You must use forward slash (/) or double backslashes in file 
# names. 

# Replicator role.  Uncomment one of the choices of master or slave.  
# There is no default for this value--it must be set or the replicator 
# will not go online.  
replicator.role=@{REPL_ROLE}

# URI to which we connect when this replicator is a slave. 
replicator.master.connect.uri=wal://@{REPL_MASTERHOST}/

# URI for our listener when we are acting as a master.  Slaves 
# use this as their connect URI.  
replicator.master.listen.uri=thl://@{HOST.HOST}:@{REPL_SVC_THL_PORT}/

# Replicator auto-enable.  If true, replicator automatically goes online 
# at start-up time. 
replicator.auto_enable=@{REPL_AUTOENABLE}

# Source ID. This required parameter is used to identify replication
# event source.  It must be unique for each replicator node.
replicator.source_id=@{EXTRACTOR.REPL_DBHOST}

# Cluster name to which the replicator belongs.
cluster.name=@{CLUSTERNAME}

# Replication service type.  Values are 'remote' or 'local'.  Local services 
# do not log updates to Tungsten catalogs.  Remote services do log them. 
replicator.service.type=@{REPL_SVC_SERVICE_TYPE}

# Service to which this replicator belongs.
service.name=@{SERVICE.DEPLOYMENT_SERVICE}

# Used by manager to create datasources dynamically
replicator.resourceJdbcUrl=@{APPLIER.REPL_DBJDBCURL}
replicator.resourceJdbcDriver=@{APPLIER.REPL_DBJDBCDRIVER}
replicator.resourceVendor=@{APPLIER.REPL_DBJDBCVENDOR}
replicator.resourcePrecedence=99

################################
# BACKUP/RESTORE CONFIGURATION #
################################

# List of configured backup agents.  Uncomment appropriately for your site. 
replicator.backup.agents=@{APPLIER.REPL_DBBACKUPAGENTS}

# Default backup agent.
replicator.backup.default=@{APPLIER.REPL_DBDEFAULTBACKUPAGENT}

@{REPL_SVC_BACKUP_CONFIG}

# List of configured storage agents.  Uncomment appropriately for your site. 
#replicator.storage.agents=fs

# Default storage agent.
replicator.storage.default=fs

# File system storage agent.  For best results the directory parameter should
# be a shared file system visible to all replicators.  NOTE: CRC file checking
# may be time-consuming for large files; it is recommended if you can afford
# to check.  (Who really wants to load a bad backup??)
replicator.storage.agent.fs=com.continuent.tungsten.replicator.backup.FileSystemStorageAgent
replicator.storage.agent.fs.directory=@{SERVICE.REPL_BACKUP_STORAGE_DIR}
replicator.storage.agent.fs.retention=@{REPL_BACKUP_RETENTION}
replicator.storage.agent.fs.crcCheckingEnabled=true

###########################
# OPEN REPLICATOR PLUGINS #
###########################

# Open replicator provider. 
replicator.plugin=script

# Script replicator properties.  Properties include the provide root directory, 
# configuration file, and script name. 
replicator.plugin.script=com.continuent.tungsten.replicator.management.script.ScriptPlugin
replicator.script.root_dir=@{CURRENT_RELEASE_DIRECTORY}/tungsten-replicator
replicator.script.conf_file=conf/postgresql-wal.properties
replicator.script.processor=bin/pg/pg-wal-plugin
