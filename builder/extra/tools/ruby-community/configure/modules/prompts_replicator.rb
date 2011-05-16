REPL_ROLE_M = "master"
REPL_ROLE_S = "slave"
REPL_ROLE_DI = "direct"
REPL_MODE_MS = "master-slave"
REPL_MODE_DI = "direct"

class DataServers < GroupConfigurePrompt
  def initialize
    super(DATASERVERS, "Enter dataserver information for @value", "dataserver", "dataservers")
    self.add_prompts(
      DatabaseHost.new(),
      DatabasePort.new(),
      DataserverLogin.new(),
      DataserverPassword.new(),
      MySQLDataDirectory.new(),
      MySQLBinlogDirectory.new(),
      MySQLBinlogPattern.new(),
      MySQLReplicationUseRelayLogs.new(),
      ReplicationServiceUseDrizzle.new(),
      
      PostgresStreamingReplication.new(),
      PostgresRootDirectory.new(),
      PostgresDataDirectory.new(),
      PostgresArchiveDirectory.new(),
      PostgresConfFile.new(),
      PostgresArchiveTimeout.new(),
      
      DatabaseInitScript.new()
    )
  end
end

class DataserverLogin < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
end

class DataserverPassword < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
  end
end

class DatabasePort < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
  end
  
  def get_default_value
    if @config.getProperty(DBMS_TYPE) == "mysql"
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
    @config.getPropertyOr(HOST, Configurator.instance.hostname())
  end
end

class DatabaseInitScript < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BOOT_SCRIPT, "MySQL start script", PV_FILENAME)
  end
  
  def get_prompt
    if @config.getProperty(DBMS_TYPE) == "mysql"
      "MySQL start script"
    else
      "Postgres start script"
    end
  end
  
  def get_default_value
    if @config.getProperty(DBMS_TYPE) == "mysql"
      "/etc/init.d/mysql"
    else
      "/etc/init.d/postgres"
    end
  end
end

class ReplicationRMIPort < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_RMI_PORT, "Replication RMI port", 
      PV_INTEGER, 10000)
  end
end

class BackupMethod < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BACKUP_METHOD, "Database backup method", nil, "none")
  end
  
  def get_prompt
    if @config.getProperty(DBMS_TYPE) == "mysql"
      "Database backup method (none|mysqldump|lvm|xtrabackup|script)"
    else
      "Database backup method (none|pg_dump|lvm|script)"
    end
  end
  
  def accept?(raw_value)
    if @config.getProperty(DBMS_TYPE) == "mysql"
      @validator = PV_MYSQL_BACKUP_METHOD
    else
      @validator = PV_PG_BACKUP_METHOD
    end
    
    super(raw_value)
  end
end

class BackupConfigurePrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def enabled?
    @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) != "none"
  end
end

class ScriptBackupConfigurePrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def enabled?
    @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "script"
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Backup permanent shared storage", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY))
      @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/backups"
    else
      ""
    end
  end
end

class BackupScriptPathConfigurePrompt < ScriptBackupConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BACKUP_SCRIPT, "What is the path to the backup script", PV_FILENAME)
  end
end

class BackupScriptCommandPrefixConfigurePrompt < ScriptBackupConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BACKUP_COMMAND_PREFIX, "What is the command prefix required for this script (typically sudo)", PV_BOOLEAN)
  end
end

class BackupScriptOnlineConfigurePrompt < ScriptBackupConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BACKUP_ONLINE, "Does this script support backing up a datasource while it is ONLINE", PV_BOOLEAN)
  end
end