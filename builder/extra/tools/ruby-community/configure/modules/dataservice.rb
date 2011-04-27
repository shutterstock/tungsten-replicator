class LocalReplicationServiceName < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(DSNAME, "What is the local service name?", PV_IDENTIFIER)
  end
end

class ReplicationServiceName < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  include DeleteServicePrompt
  
  def initialize
    super(DEPLOYMENT_SERVICE, "What replication service do you want to work with?", PV_IDENTIFIER)
  end
end

class ReplicationServiceStart < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_SVC_START, "Do you want to automatically start the service?", PV_BOOLEAN, "false")
  end
end

class ReplicationServiceType < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_SVC_SERVICE_TYPE, "What is the replication service type? (local|remote)", 
      PropertyValidator.new("local|remote",
      "Value must be local or remote"), "local")
  end
end

class ReplicationServiceRole < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_ROLE, "What is the replication role for this service? (#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_MODE_DI})",
      PropertyValidator.new("#{REPL_ROLE_M}|#{REPL_ROLE_S}|#{REPL_MODE_DI}",
      "Value must be #{REPL_ROLE_M}, #{REPL_ROLE_S} or #{REPL_MODE_DI}"), "slave")
  end
end

class ReplicationServiceMaster < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_MASTERHOST, "What is the master host for this service?", PV_IDENTIFIER)
  end
  
  def get_default_value
    if @config.getProperty(REPL_ROLE) == REPL_ROLE_M
      @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])
    else
      nil
    end
  end
  
  def enabled?
    @config.getProperty(REPL_ROLE) != REPL_ROLE_DI
  end
end

class ReplicationServiceMasterTHLPort < AdvancedPrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_MASTERPORT, 
      "Master THL port", PV_INTEGER, "2112")
  end
  
  def enabled?
    super && @config.getProperty(REPL_ROLE) != REPL_ROLE_DI
  end
end

class ReplicationServiceExtractor < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_EXTRACTOR_DATASERVER, "Datasource to extract events from", PV_IDENTIFIER)
  end
  
  def enabled?
    super && @config.getProperty(REPL_ROLE) == REPL_ROLE_DI
  end
end

class ReplicationServiceTHLPort < AdvancedPrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_SVC_THL_PORT, 
      "Port to use for THL operations", PV_INTEGER, "2112")
  end
end

class ReplicationServiceDataserver < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_DATASERVER, 
      "Which dataserver should be used for applying replication events?", PV_IDENTIFIER)
  end
end

class ReplicationShardIDMode < AdvancedPrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
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
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_SVC_ALLOW_BIDI_UNSAFE, 
      "Allow unsafe SQL from remote service (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    "false"
  end
end

class ReplicationAllowAllSQL < AdvancedPrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
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
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_AUTOENABLE, "Auto-enable services after start-up", 
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceChannels < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_SVC_CHANNELS, "Number of replication channels to use for services",
      PV_INTEGER, 1)
  end
end

class ReplicationServiceBufferSize < ConfigurePrompt
  include CreateServicePrompt
  include UpdateServicePrompt
  
  def initialize
    super(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
      PV_REPL_BUFFER_SIZE, 10)
  end
end