class ReplicationServices < GroupConfigurePrompt
  def initialize
    super(REPL_SERVICES, "Enter replication service information for @value", "replication service", "replication services", "SERVICE")
    
    ReplicationServicePrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
  
  def update_deprecated_keys()
    each_member{
      |member|
      
      host = @config.getProperty([REPL_SERVICES, member, DEPLOYMENT_HOST])
      
      if (p = @config.getProperty([HOSTS, host, REPL_THL_LOG_RETENTION])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_THL_LOG_RETENTION], p)
        @config.setProperty([HOSTS, host, REPL_THL_LOG_RETENTION], nil)
      end
      
      if (p = @config.getProperty([HOSTS, host, REPL_THL_DO_CHECKSUM])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_THL_DO_CHECKSUM], p)
        @config.setProperty([HOSTS, host, REPL_THL_DO_CHECKSUM], nil)
      end
      
      if (p = @config.getProperty([HOSTS, host, REPL_CONSISTENCY_POLICY])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_CONSISTENCY_POLICY], p)
        @config.setProperty([HOSTS, host, REPL_CONSISTENCY_POLICY], nil)
      end
      
      if (p = @config.getProperty([HOSTS, host, REPL_THL_LOG_FILE_SIZE])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_THL_LOG_FILE_SIZE], p)
        @config.setProperty([HOSTS, host, REPL_THL_LOG_FILE_SIZE], nil)
      end
      
      if (p = @config.getProperty([HOSTS, host, REPL_THL_LOG_CONNECTION_TIMEOUT])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_THL_LOG_CONNECTION_TIMEOUT], p)
        @config.setProperty([HOSTS, host, REPL_THL_LOG_CONNECTION_TIMEOUT], nil)
      end
      
      if (p = @config.getProperty([HOSTS, host, 'repl_backup_storage_dir'])).to_s != ""
        @config.setProperty([REPL_SERVICES, member, REPL_BACKUP_STORAGE_DIR], p)
        @config.setProperty([HOSTS, host, 'repl_backup_storage_dir'], nil)
      end
    }
    
    super()
  end
end

# Prompts that include this module will be collected for each dataservice 
# across interactive mode, the configure-service script and the
# tungsten-installer script
module ReplicationServicePrompt
  def get_applier_datasource
    ds = @config.getProperty(get_member_key(REPL_DATASOURCE))
    if ds.to_s() == ""
      ds = DEFAULTS
    end
    
    get_datasource_for(ds)
  end
  
  def get_extractor_datasource
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
      ds = @config.getProperty(get_member_key(REPL_MASTER_DATASOURCE))
      if ds.to_s() == ""
        ds = DEFAULTS
      end
      
      get_datasource_for(ds)
    else
      get_applier_datasource()
    end
  end
  
  def get_applier_key(key)
    [DATASOURCES, @config.getProperty(get_member_key(REPL_DATASOURCE)), key]
  end
  
  def get_extractor_key(key)
    [DATASOURCES, @config.getProperty(get_member_key(REPL_MASTER_DATASOURCE)), key]
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), key]
  end
  
  def get_datasource_for(ds_key)
    ConfigureDatabasePlatform.build(
      @config.getProperty([DATASOURCES, ds_key, REPL_DBTYPE]),
      @config.getProperty([DATASOURCES, ds_key, REPL_DBHOST]),
      @config.getProperty([DATASOURCES, ds_key, REPL_DBPORT]),
      @config.getProperty([DATASOURCES, ds_key, REPL_DBLOGIN]),
      @config.getProperty([DATASOURCES, ds_key, REPL_DBPASSWORD]), @config)
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
  
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
end

class ReplicationServiceDeploymentHost < ConfigurePrompt
  include ReplicationServicePrompt
  include GroupConfigurePromptMember
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this service?", 
      PV_IDENTIFIER)
    @weight = -1
  end
  
  def get_disabled_value
    @config.getPropertyOr(DEPLOYMENT_HOST, DIRECT_DEPLOYMENT_HOST_ALIAS)
  end
  
  def get_default_value
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def is_valid?
    super()
    
    unless @config.getProperty(HOSTS).has_key?(get_value())
      raise ConfigurePromptError.new(self, "Host #{get_value()} does not exist in the configuration file", get_value())
    end
  end
  
  def enabled_for_command_line?()
    false
  end
end

class ReplicationServiceName < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(DEPLOYMENT_SERVICE, "What is the replication service name?", 
      PV_IDENTIFIER, DEFAULT_SERVICE_NAME)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('deployment_service'))
    super()
  end
  
  def enabled_for_command_line?
    unless Configurator.instance.package.is_a?(ConfigureServicePackage)
      super() && true
    else
      false
    end
  end
end

class LocalReplicationServiceName < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(DSNAME, "What is the local service name?", PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(DEPLOYMENT_SERVICE))
  end
end

class ReplicationServiceStart < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_SVC_START, "Do you want to automatically start the service?", PV_BOOLEAN)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_SVC_REPORT))
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ReplicationServiceReport < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_SVC_REPORT, "Do you want to automatically start the service?", PV_BOOLEAN, "false")
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ReplicationServiceType < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_SVC_SERVICE_TYPE, "What is the replication service type? (local|remote)", 
      PropertyValidator.new("local|remote",
      "Value must be local or remote"), "local")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_service_type'))
    super()
  end
end

class ReplicationServiceRole < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_ROLE, "What is the replication role for this service? (#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_ROLE_DI})",
      PropertyValidator.new("#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_ROLE_DI}",
      "Value must be #{REPL_ROLE_M}, #{REPL_ROLE_S} or #{REPL_ROLE_DI}"), REPL_ROLE_S)
  end
end

class ReplicationServiceLogDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_LOG_DIR, "What is the THL directory for this service?",
      PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_SERVICE)).to_s == ""
      return nil
    end
    
    get_directory(@config.getProperty(get_member_key(DEPLOYMENT_SERVICE)))
  end
  
  def output_usage
    output_usage_line("--#{get_command_line_argument()}", get_prompt(), get_value(true, true) || get_directory('service-name'), nil, get_prompt_description())
  end
    
  def get_directory(svc_name)
    @config.getProperty([HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), REPL_LOG_DIR]) + "/" +
      svc_name
  end
end

class ReplicationServiceRelayLogDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "What is the relay log directory for this service?",
      PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_SERVICE)).to_s == ""
      return nil
    end
    
    get_directory(@config.getProperty(get_member_key(DEPLOYMENT_SERVICE)))
  end
  
  def output_usage
    output_usage_line("--#{get_command_line_argument()}", get_prompt(), get_value(true, true) || get_directory('service-name'), nil, get_prompt_description())
  end
    
  def get_directory(svc_name)
    @config.getProperty([HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), REPL_RELAY_LOG_DIR]) + "/" +
      svc_name
  end
end

class ReplicationServiceDatasource < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  include NoTemplateValuePrompt
  
  def initialize
    super(REPL_DATASOURCE, "Replication datasource", PV_ANY)
  end
  
  def get_default_value
    if (ds_keys = @config.getPropertyOr(DATASOURCES, {}).keys).size > 1
      return nil
    else
      return ds_keys[0]
    end
  end
  
  def get_prompt_description
    output = []
    
    @config.getPropertyOr(DATASOURCES, {}).keys.each {
      |ds_key|
      
      ds = get_datasource_for(ds_key)
      output << "#{ds_key}\t- #{ds.get_connection_summary}"
    }
    
    return output.join("<br>")
  end
end

class ReplicationServiceMasterDatasource < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  
  def initialize
    super(REPL_MASTER_DATASOURCE, "Replication master datasource", PV_ANY)
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI)
  end
  
  def enabled_for_config?
    super() && (@config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI)
  end
  
  def get_prompt_description
    output = []
    
    @config.getPropertyOr(DATASOURCES, {}).keys.each {
      |ds_key|
      
      ds = get_datasource_for(ds_key)
      output << "#{ds_key}\t- #{ds.get_connection_summary}"
    }
    
    return output.join("<br>")
  end
end

class ReplicationServiceTHLMaster < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_MASTERHOST, "What is the master host for this service?", 
      PV_IDENTIFIER)
  end
    
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_master_host'))
    super()
  end
end

class ReplicationServiceTHLMasterPort < ConfigurePrompt
  include ReplicationServicePrompt
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_MASTERPORT, 
      "Master THL port", PV_INTEGER)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_master_port'))
    super()
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_SVC_THL_PORT))
  end
end

class ReplicationServiceTHLPort < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER, "2112")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_thl_port'))
    super()
  end
end

class ReplicationShardIDMode < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_SHARD_DEFAULT_DB, 
      "Mode for setting the shard ID from the default db (stringent|relaxed)", 
      PropertyValidator.new("stringent|relaxed", 
      "Value must be stringent or relaxed"), "stringent")
  end
end

class ReplicationAllowUnsafeSQL < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service (true|false)", PV_BOOLEAN, "false")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_allow_bidi_unsafe'))
    super()
  end
end

class ReplicationAllowAllSQL < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_ALLOW_ANY_SERVICE, 
      "Replicate from any service (true|false)", 
      PV_BOOLEAN, "false")
  end
end

class ReplicationServiceAutoEnable < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_AUTOENABLE, "Auto-enable services after start-up", 
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceChannels < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_CHANNELS, "Number of replication channels to use for services",
      PV_INTEGER, 1)
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_channels'))
    super()
  end
end

class ReplicationServiceParallelizationType < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_TYPE, "Method for implementing parallel queues (disk|memory)",
      PropertyValidator.new("disk|memory", 
        "Value must be disk or memory"), "memory")
  end
end

class ReplicationServiceParallelizationStoreClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_STORE_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueStore"
    else
      "com.continuent.tungsten.replicator.thl.THLParallelQueue"
    end
  end
end

class ReplicationServiceParallelizationApplierClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_APPLIER_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueApplier"
    else
      "com.continuent.tungsten.replicator.thl.THLParallelQueueApplier"
    end
  end
end

class ReplicationServiceParallelizationExtractorClass < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_PARALLELIZATION_EXTRACTOR_CLASS, "Class name for parallel queue storage", PV_ANY)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_SVC_PARALLELIZATION_TYPE)) == "memory"
      "com.continuent.tungsten.replicator.storage.parallel.ParallelQueueExtractor"
    else
      "com.continuent.tungsten.replicator.thl.THLParallelQueueExtractor"
    end
  end
end

class ReplicationServiceBufferSize < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
      PV_REPL_BUFFER_SIZE, 10)
  end
end

class ReplicationServiceSlaveTakeover < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_SVC_NATIVE_SLAVE_TAKEOVER, "Takeover native replication",
      PV_BOOLEAN, "false")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_svc_native_slave_takeover'))
    super()
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class BackupMethod < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_BACKUP_METHOD, "Database backup method", nil)
  end
  
  def get_default_value
    get_applier_datasource().get_default_backup_method()
  end
  
  def get_prompt
    "Database backup method (#{get_applier_datasource().get_valid_backup_methods()})"
  end
  
  def accept?(raw_value)
    @validator = PropertyValidator.new(get_applier_datasource().get_valid_backup_methods(),
      "Value must be #{get_applier_datasource().get_valid_backup_methods().split('|').join(', ')}")    
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

class ReplicationServiceBackupStorageDirectory < BackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  include NotTungstenInstallerPrompt
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Backup permanent shared storage", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(DEPLOYMENT_SERVICE)).to_s == ""
      return nil
    end
    
    get_backup_directory(@config.getProperty(get_member_key(DEPLOYMENT_SERVICE)))
  end
  
  def output_usage
    output_usage_line("--#{get_command_line_argument()}", get_prompt(), get_value(true, true) || get_backup_directory('service-name'), nil, get_prompt_description())
  end
    
  def get_backup_directory(svc_name)
    @config.getProperty([HOSTS, @config.getProperty(get_member_key(DEPLOYMENT_HOST)), REPL_BACKUP_STORAGE_DIR]) + "/" +
      svc_name
  end
end

class BackupStorageTempDirectory < BackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory", PV_FILENAME, "/tmp")
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_backup_dump_dir'))
    super()
  end
end

class BackupStorageRetention < BackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_BACKUP_RETENTION, "Number of backups to retain", PV_INTEGER, "3")
  end
end

class BackupScriptPathConfigurePrompt < ScriptBackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_BACKUP_SCRIPT, "What is the path to the backup script", PV_FILENAME)
  end
end

class BackupScriptCommandPrefixConfigurePrompt < ScriptBackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_BACKUP_COMMAND_PREFIX, "Use sudo when running the backup script?", PV_BOOLEAN, "false")
  end
  
  def get_template_value(transform_values_method)
    if get_value() == "true"
      "sudo"
    else
      ""
    end
  end
end

class BackupScriptOnlineConfigurePrompt < ScriptBackupConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_BACKUP_ONLINE, "Does the backup script support backing up a datasource while it is ONLINE", PV_BOOLEAN, "false")
  end
end

class THLStorageChecksum < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_DO_CHECKSUM, "Execute checksum operations on THL log files", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConnectionTimeout < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_CONNECTION_TIMEOUT, "Number of seconds to wait for a connection to the THL log", 
      PV_INTEGER, "600")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageFileSize < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_FILE_SIZE, "File size in bytes for THL disk logs", 
      PV_INTEGER, "100000000")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageRetention < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_THL_LOG_RETENTION, "How long do you want to keep THL files?", 
      PV_ANY, "7d")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConsistency < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_CONSISTENCY_POLICY, "Should the replicator stop or warn if a consistency check fails?", 
      PV_ANY, "stop")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class ReplicationServiceConfigFile < ConfigurePrompt
  include ReplicationServicePrompt
  include HiddenValueModule
  
  def initialize
    super(REPL_SVC_CONFIG_FILE, "Path to replication service static properties file", 
      PV_FILENAME)
  end
  
  def get_default_value
    "#{@config.getProperty(CURRENT_RELEASE_DIRECTORY)}/tungsten-replicator/conf/static-#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}.properties"
  end
end

class ReplicationServiceApplierConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_APPLIER_CONFIG, "Replication service applier config properties")
  end
  
  def get_template_value(transform_values_method)
    transformer = Transformer.new(@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/" + 
      get_applier_datasource().get_applier_template())
    transformer.transform_values(transform_values_method)
    return transformer.to_s
  end
end

class ReplicationServiceExtractorConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_EXTRACTOR_CONFIG, "Replication service extractor config properties")
  end
  
  def get_template_value(transform_values_method)
    transformer = Transformer.new(@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/" + 
      get_extractor_datasource().get_extractor_template())
    transformer.transform_values(transform_values_method)
    return transformer.to_s
  end
end

class ReplicationServiceFilterConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_FILTER_CONFIG, "Replication service filter config properties")
  end
  
  def get_template_value(transform_values_method)
    output_lines = []
    
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/tungsten-replicator/samples/conf/filters/default/*.tpl'].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    if get_applier_datasource().class != get_extractor_datasource.class
      Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/tungsten-replicator/samples/conf/filters/#{get_extractor_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
        transformer = Transformer.new(file)
        transformer.transform_values(transform_values_method)
      
        output_lines = output_lines + transformer.to_a + [""]
      end
    end
    
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/tungsten-replicator/samples/conf/filters/#{get_applier_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    return output_lines.join("\n")
  end
end

class ReplicationServiceBackupConfig < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_SVC_BACKUP_CONFIG, "Replication service backup config properties")
  end
  
  def get_template_value(transform_values_method)
    output_lines = []
    
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/tungsten-replicator/samples/conf/backup_methods/default/*.tpl'].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + "/tungsten-replicator/samples/conf/backup_methods/#{get_applier_datasource().get_uri_scheme()}/*.tpl"].sort().each do |file| 
      transformer = Transformer.new(file)
      transformer.transform_values(transform_values_method)
      
      output_lines = output_lines + transformer.to_a + [""]
    end
    
    return output_lines.join("\n")
  end
end

class ReplicationServiceExtractorFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_EXTRACTOR_FILTERS, "Replication service extractor filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_extractor_datasource().get_extractor_filters()).join(",")
  end
  
  def required?
    false
  end
end

class ReplicationServiceTHLFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_THL_FILTERS, "Replication service THL filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_extractor_datasource().get_thl_filters()).join(",")
  end
  
  def required?
    false
  end
end

class ReplicationServiceApplierFilters < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_SVC_APPLIER_FILTERS, "Replication service applier filters")
  end
  
  def get_template_value(transform_values_method)
    (get_value().to_s().split(",") + get_applier_datasource().get_applier_filters()).join(",")
  end
  
  def required?
    false
  end
end