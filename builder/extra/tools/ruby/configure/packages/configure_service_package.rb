class ConfigureServicePackage < ConfigurePackage
  SERVICE_CREATE = "create_service"
  SERVICE_DELETE = "delete_service"
  SERVICE_UPDATE = "update_service"
  
  def initialize(config)
    super(config)
    @remainder_option_property = DEPLOYMENT_SERVICE
  end
  
  def prepare_parser(opts)
    configurator = Configurator.instance
    
    unless configurator.command_properties.has_key?(DEPLOYMENT_TYPE)
      configurator.command_properties[DEPLOYMENT_TYPE] = "create_service"
    end
    
    configurator.command_properties[DEPLOY_PACKAGE_URI] = nil
    configurator.command_properties[DEPLOY_CURRENT_PACKAGE] = nil
    
    opts.on("-C", "--create")   { configurator.command_properties[DEPLOYMENT_TYPE] = SERVICE_CREATE }
    opts.on("-D", "--delete")   { configurator.command_properties[DEPLOYMENT_TYPE] = SERVICE_DELETE }
    opts.on("-U", "--update")   { configurator.command_properties[DEPLOYMENT_TYPE] = SERVICE_UPDATE }
    opts.on("--start")          { configurator.command_properties[REPL_SVC_START] = "true" }
  end
  
  def allowed_property_options
    {
      "--local-service-name" => DSNAME,
      "--role" => REPL_ROLE,
      "--master-host" => REPL_MASTERHOST,
      "--master-port" => REPL_MASTERPORT,
      "--direct-datasource" => REPL_EXTRACTOR_DATASERVER,
      "--datasource" => REPL_DATASERVER,
      "--auto-enable" => REPL_AUTOENABLE,
      "--buffer-size" => REPL_BUFFER_SIZE,
      "--channels" => REPL_SVC_CHANNELS,
      "--thl-port" => REPL_SVC_THL_PORT,
      "--shard-default-db" => REPL_SVC_SHARD_DEFAULT_DB,
      "--allow-bidi-unsafe" => REPL_SVC_ALLOW_BIDI_UNSAFE,
      "--allow-any-remote-service" => REPL_SVC_ALLOW_ANY_SERVICE,
      "--service-type" => REPL_SVC_SERVICE_TYPE
    }
  end
  
  def output_usage
    puts "Usage: configure-service {-C|-D|-U} [options] service-name"
    output_general_usage()
    Configurator.instance.write_divider()
    puts "Service options"
    puts "--allow-bidi-unsafe         Allow unsafe SQL from remote service [#{@config.getProperty(REPL_SVC_ALLOW_BIDI_UNSAFE)}]"
    puts "--allow-any-remote-service  Replicate from any service [#{@config.getProperty(REPL_SVC_ALLOW_ANY_SERVICE)}]"
    puts "--auto-enable               If true, service goes online at startup [#{@config.getProperty(REPL_AUTOENABLE)}]"
    puts "--buffer-size               Size of buffers for block commit and queues [#{@config.getProperty(REPL_BUFFER_SIZE)}]"
    puts "--channels                  Number of channels for parallel apply [#{@config.getProperty(REPL_SVC_CHANNELS)}]"
    puts "--local-service-name        Replicator service that owns master [#{@config.getProperty(DSNAME)}]"
    puts "--master-host               Replicator remote master host name [#{@config.getProperty(REPL_MASTERHOST)}]"
    puts "--master-port               Replicator remote master THL port [#{@config.getProperty(REPL_MASTERPORT)}]"
    puts "--role                      Replicator role [#{@config.getProperty(REPL_ROLE)}]"
    puts "--service-type              Replicator service type (local|remote) [#{@config.getProperty(REPL_SVC_SERVICE_TYPE)}]"
    puts "--shard-default-db          Use default db for shard ID (stringent|relaxed) [#{@config.getProperty(REPL_SVC_SHARD_DEFAULT_DB)}]"
  end
  
  def get_prompts
    [
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
    ]
  end
  
  def get_non_interactive_prompts
    ConfigurePackageCluster.new(@config).get_prompts()
  end
  
  def store_config_file?
    false
  end
end

module CreateServicePrompt
  def enabled?
    super() && get_service_deployment_types().include?(@config.getProperty(DEPLOYMENT_TYPE))
  end
  
  def get_service_deployment_types
    begin
      prev = super()
    rescue
      prev = []
    end
    
    prev + [ConfigureServicePackage::SERVICE_CREATE]
  end
end

module UpdateServicePrompt
  def enabled?
    super() && get_service_deployment_types().include?(@config.getProperty(DEPLOYMENT_TYPE))
  end
  
  def get_service_deployment_types
    begin
      prev = super()
    rescue
      prev = []
    end
    
    prev + [ConfigureServicePackage::SERVICE_UPDATE]
  end
end

module DeleteServicePrompt
  def enabled?
    super() && get_service_deployment_types().include?(@config.getProperty(DEPLOYMENT_TYPE))
  end
  
  def get_service_deployment_types
    begin
      prev = super()
    rescue
      prev = []
    end
    
    prev + [ConfigureServicePackage::SERVICE_DELETE]
  end
end