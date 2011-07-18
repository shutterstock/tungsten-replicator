require 'configure/modules/prompts_deployment.rb'

REPL_ROLE_M = "master"
REPL_ROLE_S = "slave"
REPL_ROLE_DI = "direct"
REPL_MODE_MS = "master-slave"
REPL_MODE_DI = "direct"

class Dataservers < GroupConfigurePrompt
  def initialize
    super(DATASERVERS, "Enter dataserver information for @value", "dataserver", "dataservers")

    DataserverPrompt.subclasses().each{
      |klass|
      
      begin
        self.add_prompt(klass.new())
      rescue => e
        if klass.subclasses()
          klass.subclasses().each {
            |subklass|
            self.add_prompt(subklass.new())
          }
        end
      end
    }
  end
end

module DataserverPrompt
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
end

class DBMSTypePrompt < ConfigurePrompt
  include DataserverPrompt
  
  def initialize
    dbms_types = DBMSTypePrompt.dbms_types()
    
    validator = PropertyValidator.new(dbms_types.join("|"), 
      "Value must be #{dbms_types.join(',')}")
      
    super(DBMS_TYPE, "Database type (#{dbms_types.join(',')})", 
        validator)
  end
  
  def get_default_value
    case Configurator.instance.whoami()
    when "postgres"
      return "postgresql"
    when "enterprisedb"
      return "postgresql"
    else
      return "mysql"
    end
  end
  
  def self.add_dbms_type(type)
    @dbms_types ||= []
    @dbms_types << type
  end

  def self.dbms_types
    @dbms_types || []
  end
end

class DatabaseHost < ConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_DBHOST, "Database server hostname", PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getPropertyOr(HOST, Configurator.instance.hostname())
  end
end

class DatabasePort < ConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      3306
    else
      5432
    end
  end
end

class DataserverLogin < ConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
end

class DataserverPassword < ConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
  end
end

class DatabaseInitScript < ConfigurePrompt
  include DataserverPrompt

  def initialize
    super(REPL_BOOT_SCRIPT, "MySQL start script", PV_FILENAME)
  end
  
  def get_prompt
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      "MySQL start script"
    else
      "Postgres start script"
    end
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      "/etc/init.d/mysql"
    else
      "/etc/init.d/postgres"
    end
  end
end

class BackupMethod < ConfigurePrompt
  include DataserverPrompt

  def initialize
    super(REPL_BACKUP_METHOD, "Database backup method", nil)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      "mysqldump"
    else
      "pg_dump"
    end
  end
  
  def get_prompt
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      "Database backup method (none|mysqldump|xtrabackup|script)"
    else
      "Database backup method (none|pg_dump|script)"
    end
  end
  
  def accept?(raw_value)
    if @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
      @validator = PV_MYSQL_BACKUP_METHOD
    else
      @validator = PV_PG_BACKUP_METHOD
    end
    
    super(raw_value)
  end
end

class BackupConfigurePrompt < ConfigurePrompt
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) != "none"
  end
end

class ScriptBackupConfigurePrompt < ConfigurePrompt
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "script"
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Backup permanent shared storage", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY))
      @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/backups"
    else
      nil
    end
  end
  
  def required?
    false
  end
end

class BackupStorageTempDirectory < BackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory", PV_FILENAME, "/tmp")
  end
end

class BackupStorageRetention < BackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_RETENTION, "Number of backups to retain", PV_INTEGER, "3")
  end
end

class BackupScriptPathConfigurePrompt < ScriptBackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_SCRIPT, "What is the path to the backup script", PV_FILENAME)
  end
end

class BackupScriptCommandPrefixConfigurePrompt < ScriptBackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_COMMAND_PREFIX, "What is the command prefix required for this script (typically sudo)", PV_BOOLEAN)
  end
end

class BackupScriptOnlineConfigurePrompt < ScriptBackupConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_BACKUP_ONLINE, "Does this script support backing up a datasource while it is ONLINE", PV_BOOLEAN)
  end
end

class ReplicationAPI < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API, "Enable the replication API", PV_BOOLEAN, "false")
  end
end

class ReplicationAPIHost < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_HOST, "Hostname that the replication API should listen on", PV_HOSTNAME, "localhost")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPort < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_PORT, "Port that the replication API should bind to", PV_INTEGER, "19999")
  end

  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIUser < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_USER, "HTTP basic auth username for the replication API", PV_ANY, "tungsten")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPassword < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_PASSWORD, "HTTP basic auth password for the replication API", PV_ANY, "secret")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end