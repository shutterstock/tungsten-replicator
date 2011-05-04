class LocalReplicationServiceName < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(DSNAME, "What is the local service name?", PV_IDENTIFIER)
  end
end

class ReplicationServiceName < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(DEPLOYMENT_SERVICE, "What replication service do you want to work with?", PV_IDENTIFIER)
  end
end

class ReplicationServiceStart < ConstantValuePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_START, "Do you want to automatically start the service?", PV_BOOLEAN, "false")
  end
end

class ReplicationServiceType < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_SERVICE_TYPE, "What is the replication service type? (local|remote)", 
      PropertyValidator.new("local|remote",
      "Value must be local or remote"), "local")
  end
end

class ReplicationServiceRole < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_ROLE, "What is the replication role for this service? (#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_MODE_DI})",
      PropertyValidator.new("#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_MODE_DI}",
      "Value must be #{REPL_ROLE_M}, #{REPL_ROLE_S} or #{REPL_MODE_DI}"), "slave")
  end
end

class ReplicationServiceMaster < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MASTERHOST, "What is the master host for this service?", PV_IDENTIFIER)
  end
    
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
  end
end

class ReplicationServiceMasterTHLPort < AdvancedPrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MASTERPORT, 
      "Master THL port", PV_INTEGER, "2112")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_S
  end
end

class ReplicationServiceExtractor < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_EXTRACTOR_DATASERVER, "Datasource to extract events from", PV_IDENTIFIER)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
  end
  
  def is_valid?
    super() && @config.getProperty(DATASERVERS).has_key?(get_value())
  end
end

class ReplicationServiceTHLPort < AdvancedPrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER, "2112")
  end
end

class ReplicationServiceDataserver < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_DATASERVER, 
      "Which dataserver should be used for applying replication events?", PV_IDENTIFIER)
  end
  
  def is_valid?
    super()
    
    if enabled?
      unless @config.getProperty(DATASERVERS).has_key?(get_value())
        raise ConfigurePromptError.new(self, "Datasource #{get_value()} does not exist in the configuration file", get_value())
      end
    end
  end
end

class ReplicationShardIDMode < AdvancedPrompt
  include NotDeleteServicePrompt
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

class ReplicationAllowUnsafeSQL < AdvancedPrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    "false"
  end
end

class ReplicationAllowAllSQL < AdvancedPrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_ALLOW_ANY_SERVICE, 
      "Replicate from any service (true|false)", 
      PV_BOOLEAN)
  end
  
  def get_default_value
    "false"
  end
end

class ReplicationServiceAutoEnable < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_AUTOENABLE, "Auto-enable services after start-up", 
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceChannels < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_SVC_CHANNELS, "Number of replication channels to use for services",
      PV_INTEGER, 1)
  end
end

class ReplicationServiceBufferSize < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
      PV_REPL_BUFFER_SIZE, 10)
  end
end

class ReplicationServiceDeploymentHost < ConfigurePrompt
  include GroupConfigurePromptMember

  def initialize
    super(DEPLOYMENT_HOST, 
      "On what host would you like to deploy this service?", 
      PV_IDENTIFIER)
  end
  
  def enabled?
    super() && @config.getProperty(DEPLOYMENT_TYPE) == DISTRIBUTED_DEPLOYMENT_NAME
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
end

class ReplicationServices < GroupConfigurePrompt
  def initialize
    super(REPL_SERVICES, "Enter replication service information for @value", "replication service", "replication services")
    
    self.add_prompts(
      ReplicationServiceDeploymentHost.new(),
      LocalReplicationServiceName.new(),
      ReplicationServiceName.new(),
      ReplicationServiceType.new(),
      ReplicationServiceRole.new(),
      ReplicationServiceMaster.new(),
      ReplicationServiceMasterTHLPort.new(),
      ReplicationServiceExtractor.new(),
      ReplicationServiceDataserver.new(),
      ReplicationServiceTHLPort.new(),
      ReplicationServiceAutoEnable.new(),
      ReplicationServiceChannels.new(),
      ReplicationServiceBufferSize.new(),
      ReplicationShardIDMode.new(),
      ReplicationAllowUnsafeSQL.new(),
      ReplicationAllowAllSQL.new(),
      ReplicationServiceStart.new()
    )
  end
  
  def get_prompt
    if @config.getProperty(DBMS_TYPE) == DBMS_POSTGRESQL
      "Enter a name for the replication service"
    else
      "Enter a comma-delimited list of names for the replication services"
    end
  end
  
  def get_description
    if @config.getProperty(DBMS_TYPE) == DBMS_POSTGRESQL
      "Replication services are the pipeline for replicating data between hosts.  Tungsten Replicator for PostgreSQL supports a single replication service for master-slave replication between PostgreSQL hosts."
    else
      "Replication services are the pipeline for replicating data between hosts.  Tungsten Replicator for MySQL supports multiple replication services to create complex replication topologies between MySQL hosts."
    end
  end
  
  def accept?(raw_value)
    value = super(raw_value)
    
    if @config.getProperty(DBMS_TYPE) == DBMS_POSTGRESQL
      if value.to_s().index(",") != nil
        raise PropertyValidatorException, "Tungsten Replicator for PostgreSQL only supports a single replication service"
      end
    end
    
    value
  end
end