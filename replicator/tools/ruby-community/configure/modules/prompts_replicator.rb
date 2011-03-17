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

class VIPConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(REPL_MASTER_VIP) != "none" && @config.getProperty(REPL_MASTER_VIP) != ""
  end
end

class ReadOnlyReplicator < ConfigurePrompt
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

class DatabaseInitScript < ConfigurePrompt
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