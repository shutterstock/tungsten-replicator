class Datasources < GroupConfigurePrompt
  def initialize
    super(DATASOURCES, "Enter datasource information for @value", "datasource", "datasources", "APPLIER|EXTRACTOR")
    
    DatasourcePrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
  
  def update_deprecated_keys()
    if @config.getProperty('dataservers') != nil
      @config.setProperty(DATASOURCES, @config.getNestedProperty(['dataservers']))
      @config.setProperty('dataservers', nil)
    end
    
    each_member{
      |member|
      
      @config.setProperty([DATASOURCES, member, 'repl_use_drizzle'], nil)
    }
    super()
  end
end

# Prompts that include this module will be collected for each datasource 
# across interactive mode, the tungsten-installer script
module DatasourcePrompt
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_datasource
    ConfigureDatabasePlatform.build(@config.getProperty(get_member_key(REPL_DBTYPE)),
      @config.getProperty(get_member_key(REPL_DBHOST)),
      @config.getProperty(get_member_key(REPL_DBPORT)),
      @config.getProperty(get_member_key(REPL_DBLOGIN)),
      @config.getProperty(get_member_key(REPL_DBPASSWORD)), @config)
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def get_host_alias
    host_alias = nil
    @config.getPropertyOr(HOSTS, {}).keys.each{
      |h_key|
      if @config.getProperty([HOSTS, h_key, HOST]) == @config.getProperty(get_member_key(REPL_DBHOST))
        host_alias = h_key
      end
    }
    
    host_alias
  end
  
  def get_userid
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, USERID])
  end
  
  def get_hostname
    host_alias = get_host_alias()
    if host_alias == nil
      super
    end
    
    @config.getProperty([HOSTS, host_alias, HOST])
  end
end

class DatasourceDBType < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    validator = PropertyValidator.new(ConfigureDatabasePlatform.get_types().join("|"), 
      "Value must be #{ConfigureDatabasePlatform.get_types().join(',')}")
      
    super(REPL_DBTYPE, "Database type (#{ConfigureDatabasePlatform.get_types().join(',')})", 
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
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('dbms_type'))
    super()
  end
end

class DatasourceDBHost < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBHOST, "Database server hostname", PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getPropertyOr(HOST, Configurator.instance.hostname())
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_dbhost'))
    super()
  end
end

class DatasourceDBPort < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBPORT, "Database server port", PV_INTEGER)
  end
  
  def get_default_value
    get_datasource().get_default_port()
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_dbport'))
    super()
  end
end

class DatasourceDBUser < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBLOGIN, "Database login for Tungsten", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_login'))
    super()
  end
end

class DatasourceDBPassword < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DBPASSWORD, "Database password", 
      PV_ANY, "")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_admin_password'))
    super()
  end
end

class DatasourceInitScript < ConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_BOOT_SCRIPT, "Database start script", PV_FILENAME)
  end
  
  def get_default_value
    get_datasource.get_default_start_script()
  end
  
  def required?
    (get_datasource.get_default_start_script() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_boot_script'))
    super()
  end
end

class DatasourceMasterLogDirectory < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGDIR, "Master log directory", 
      PV_FILENAME)
  end
  
  def get_default_value
    get_datasource().get_default_master_log_directory()
  end
  
  def required?
    (get_datasource().get_default_master_log_directory() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_dir'))
    super()
  end
end

class DatasourceMasterLogPattern < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MASTER_LOGPATTERN, "Master log filename pattern", PV_ANY)
  end
  
  def get_default_value
    get_datasource().get_default_master_log_pattern()
  end

  def required?
    (get_datasource().get_default_master_log_pattern() != nil)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_binlog_pattern'))
    super()
  end
end

class DatasourceDisableRelayLogs < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_DISABLE_RELAY_LOGS, "Disable the use of relay-logs?",
      PV_BOOLEAN, "false")
  end
  
  def update_deprecated_keys
    if @config.getProperty(get_member_key('repl_extractor_use_relay_logs')).to_s != ""
      if @config.getProperty(get_member_key('repl_extractor_use_relay_logs')) == "false"
        @config.setProperty(get_member_key(REPL_DISABLE_RELAY_LOGS), "true")
      else
        @config.setProperty(get_member_key(REPL_DISABLE_RELAY_LOGS), "false")
      end
      @config.setProperty(get_member_key('repl_extractor_use_relay_logs'), nil)
    end
  end
  
  def enabled_for_command_line?
    false
  end
  
  def get_template_value(transform_values_method)
    v = super(transform_values_method)
    
    if v == "false"
      "true"
    else
      "false"
    end
  end
end

class DatasourceTHLURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBTHLURL, "Datasource THL URL")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_thl_uri()
  end
end

class DatasourceBasicJDBCURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBBASICJDBCURL, "Datasource Basic JDBC URL")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getBasicJdbcUrl()
  end
end

class DatasourceJDBCURL < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCURL, "Datasource JDBC URL")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getJdbcUrl()
  end
end

class DatasourceJDBCDriver < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCDRIVER, "Datasource JDBC Driver")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getJdbcDriver()
  end
end

class DatasourceVendor < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBJDBCVENDOR, "Datasource Vendor")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().getVendor()
  end
end

class DatasourceBackupAgents < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBBACKUPAGENTS, "Datasource Backup Agents")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_backup_agents().join(",")
  end
end

class DatasourceDefaultBackupAgent < ConfigurePrompt
  include DatasourcePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_DBDEFAULTBACKUPAGENT, "Datasource Default Backup Agent")
  end
  
  def get_template_value(transform_values_method)
    get_datasource().get_default_backup_agent()
  end
end