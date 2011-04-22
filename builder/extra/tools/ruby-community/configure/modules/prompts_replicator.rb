class DataServers < GroupConfigurePrompt
  def initialize
    super("dataservers", "Enter dataserver information for @value", "dataserver", "dataservers")
    self.add_prompts(
      DatabaseHost.new(),
      DatabasePort.new(),
      DataserverLogin.new(),
      DataserverPassword.new(),
      MySQLBinlogDirectory.new(),
      MySQLBinlogPattern.new(),
      DatabaseInitScript.new(),
      
      PostgresStreamingReplication.new(),
      PostgresRootDirectory.new(),
      PostgresDataDirectory.new(),
      PostgresArchiveDirectory.new(),
      PostgresConfFile.new(),
      PostgresArchiveTimeout.new()
    )
  end
end

class DataserverLogin < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, "tungsten")
  end
end

class DataserverPassword < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "secret")
  end
end

class ReplicationServices < GroupConfigurePrompt
  def initialize
    super("services", "Enter replication service information for @value", "replication service", "replication services")
    self.add_prompts(
    )
  end
  
  def get_prompt
    if @config.getProperty(GLOBAL_DBMS_TYPE) == DBMS_POSTGRESQL
      "Enter a name for the replication service"
    else
      "Enter a comma-delimited list of names for the replication services"
    end
  end
  
  def get_description
    if @config.getProperty(GLOBAL_DBMS_TYPE) == DBMS_POSTGRESQL
      "Replication services are the pipeline for replicating data between hosts.  Tungsten Replicator for PostgreSQL supports a single replication service for master-slave replication between PostgreSQL hosts."
    else
      "Replication services are the pipeline for replicating data between hosts.  Tungsten Replicator for MySQL supports multiple replication services to create complex replication topologies between MySQL hosts."
    end
  end
  
  def accept?(raw_value)
    value = super(raw_value)
    
    if @config.getProperty(GLOBAL_DBMS_TYPE) == DBMS_POSTGRESQL
      if value.to_s().index(",") != nil
        raise PropertyValidatorException, "Tungsten Replicator for PostgreSQL only supports a single replication service"
      end
    end
    
    value
  end
end

class ReplicationServiceMaster < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MASTERHOST, "Enter the master host for service @value", 
    PV_IDENTIFIER)
  end
  
  def enabled?
    @config.getProperty(get_member_key(REPL_SVC_MODE)) == "master-slave"
  end
end

class ReplicationServiceHosts < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_HOSTS, 
      "Enter the replication hosts (including the master) for service @value", 
      PV_IDENTIFIER)
  end
  
  def enabled_for_source?
    @config.getProperty(get_member_key(REPL_SVC_MODE)) == "master-slave"
  end
end

class ReplicationServiceRemoteHosts < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_REMOTE_HOSTS, 
      "Which of the @value replication hosts will be used as a master for another service?", 
      PV_IDENTIFIER)
  end
  
  def required?
    false
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty(get_member_key(REPL_SVC_MODE)) == "master-slave"
  end
end

class ReplicationShardIDMode < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_SHARD_DEFAULT_DB, 
      "Mode for setting the shard ID from the default db (stringent|relaxed)", 
      PropertyValidator.new("stringent|relaxed", 
      "Value must be stringent or relaxed"))
  end
  
  def get_default_value
    "stringent"
  end
end

class ReplicationServiceShardIDMode < ConfigurePrompt
  include GroupConfigurePromptMember
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, REPL_SVC_SHARD_DEFAULT_DB, 
      "Mode for setting the shard ID from the default db for service @value (stringent|relaxed)", 
      PropertyValidator.new("stringent|relaxed", 
      "Value must be stringent or relaxed"))
  end
  
  def get_default_value
    @config.getProperty(REPL_SVC_SHARD_DEFAULT_DB)
  end
  
  def required?
    false
  end
end

class ReplicationAllowUnsafeSQL < AdvancedPrompt
  def initialize
    super(REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    "false"
  end
end

class ReplicationServiceAllowUnsafeSQL < MultipleValueConfigurePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service for service @value (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    @config.getProperty(REPL_SVC_ALLOW_BIDI_UNSAFE)
  end
  
  def required?
    false
  end
end

class ReplicationAllowAllSQL < AdvancedPrompt
  def initialize
    super(REPL_SVC_ALLOW_ANY_SERVICE, 
      "Replicate from any service (true|false)", 
      PV_BOOLEAN)
  end
  
  def get_default_value
    "false"
  end
end

class ReplicationServiceAllowAllSQL < MultipleValueConfigurePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, REPL_SVC_ALLOW_ANY_SERVICE, 
      "Replicate from any service for service @value (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    @config.getProperty(REPL_SVC_ALLOW_ANY_SERVICE)
  end
  
  def required?
    false
  end
end

class ReplicationServiceTHLPort < MultipleValueConfigurePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, REPL_SVC_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER, 2112)
  end
  
  def get_default_value
    @config.getProperty(REPL_SVC_THL_PORT)
  end
  
  def required?
    false
  end
end

class ReplicatorHostsPrompt < AdvancedPrompt
  def initialize
    super(REPL_HOSTS, "Enter a comma-delimited list of replicator hosts", 
      PV_HOSTNAME)
  end
  
  def get_default_value
    @config.getProperty(GLOBAL_HOSTS)
  end
  
  def get_disabled_value
    @config.getProperty(GLOBAL_HOSTS)
  end
end

class ReplicationServiceAutoEnable < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_AUTOENABLE, "Auto-enable service @value after start-up", 
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceChannels < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_SVC_CHANNELS, "Number of replication channels to use for service @value",
      PV_INTEGER, 1)
  end
end

class THLStorageType < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_LOG_TYPE, "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, "disk")
  end
  
  def enabled?
    Configurator.instance.advanced_mode?()
  end
  
  def get_disabled_value
    "disk"
  end
end

class THLStorageDirectory < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory on @value", PV_FILENAME)
  end
  
  def enabled?
    @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_HOME_DIRECTORY)
      @config.getProperty(GLOBAL_HOME_DIRECTORY) + "/thl-logs"
    else
      ""
    end
  end
end

class THLStorageChecksum < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_THL_DO_CHECKSUM, "Execute checksum operations on THL log files", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConnectionTimeout < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_THL_LOG_CONNECTION_TIMEOUT, "Number of seconds to wait for a connection to the THL log", 
      PV_INTEGER, 600)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageRetention < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_THL_LOG_RETENTION, "How long do you want to keep THL files?", 
      PV_ANY, "7d")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConsistency < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_CONSISTENCY_POLICY, "Should the replicator stop or warn if a consistency check fails?", 
      PV_ANY, "stop")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageFileSize < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_THL_LOG_FILE_SIZE, "File size in bytes for THL disk logs", 
      PV_INTEGER, 1000000000)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class ReplicationServiceTHLPort < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_SVC_THL_PORT, "Port to use for THL operations", 
      PV_INTEGER, 2112)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class VIPConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(REPL_MASTER_VIP) != "none" && @config.getProperty(REPL_MASTER_VIP) != ""
  end
end

class ReadOnlyReplicator < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_RO_SLAVE, "Make MySQL server read-only when acting as slave",
      PV_BOOLEAN, "true")
  end
  
  def enabled?
    Configurator.instance.tungsten_version() == Configurator::TUNGSTEN_ENTERPRISE
  end
  
  def get_disabled_value
    "false"
  end
end

class DatabasePort < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      3306
    else
      5432
    end
  end
end

class DatabaseHost < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBHOST, "Database server hostname", PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getPropertyOr(GLOBAL_HOST, "")
  end
end

class DatabaseInitScript < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BOOT_SCRIPT, "MySQL start script", PV_FILENAME)
  end
  
  def get_prompt
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      "MySQL start script"
    else
      "Postgres start script"
    end
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      "/etc/init.d/mysql"
    else
      "/etc/init.d/postgres"
    end
  end
end

class BackupMethod < ConfigurePrompt
  def initialize
    super(REPL_BACKUP_METHOD, "Database backup method")
  end
  
  def get_prompt
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      "Database backup method (none|mysqldump|lvm|xtrabackup|script)"
    else
      "Database backup method (none|pg_dump|lvm|script)"
    end
  end
  
  def accept?(raw_value)
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      @validator = PV_MYSQL_BACKUP_METHOD
    else
      @validator = PV_PG_BACKUP_METHOD
    end
    
    super(raw_value)
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      "mysqldump"
    else
      "pg_dump"
    end
  end
end

class BackupConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(REPL_BACKUP_METHOD) != "none"
  end
end

class ScriptBackupConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(REPL_BACKUP_METHOD) == "script"
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Backup permanent shared storage", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_HOME_DIRECTORY)
      @config.getProperty(GLOBAL_HOME_DIRECTORY) + "/backups"
    else
      ""
    end
  end
end

class BackupScriptPathConfigurePrompt < ScriptBackupConfigurePrompt
  def initialize
    super(REPL_BACKUP_SCRIPT, "What is the path to the backup script", PV_FILENAME)
  end
end

class BackupScriptCommandPrefixConfigurePrompt < ScriptBackupConfigurePrompt
  def initialize
    super(REPL_BACKUP_COMMAND_PREFIX, "What is the command prefix required for this script (typically sudo)", PV_BOOLEAN)
  end
end

class BackupScriptOnlineConfigurePrompt < ScriptBackupConfigurePrompt
  def initialize
    super(REPL_BACKUP_ONLINE, "Does this script support backing up a datasource while it is ONLINE", PV_BOOLEAN)
  end
end