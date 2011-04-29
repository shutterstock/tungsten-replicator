class ConfigureServicePackage < ConfigurePackage
  SERVICE_CREATE = "create_service"
  SERVICE_DELETE = "delete_service"
  SERVICE_UPDATE = "update_service"
  
  def parsed_options?(arguments)
    @config.setProperty(DEPLOYMENT_TYPE, nil)
    @config.setProperty(DEPLOY_CURRENT_PACKAGE, nil)
    @config.setProperty(DEPLOY_PACKAGE_URI, nil)
    
    opts=OptionParser.new
    
    opts.on("-C", "--create")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_CREATE) }
    opts.on("-D", "--delete")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_DELETE) }
    opts.on("-U", "--update")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_UPDATE) }
    opts.on("--start")          { @config.setProperty(REPL_SVC_START, "true") }
    
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
    }.each{
      |arg, prop_key|
      opts.on("#{arg} String")  {|val|  @config.setProperty(prop_key, val) }
    }
    
    begin
      service_name = opts.order!(arguments)
      
      unless @config.getProperty(DEPLOYMENT_TYPE)
        error("You must specify -C, -D or -U")
      end
      
      case service_name.size()
      when 0
        raise "No service_name specified"
      when 1
        @config.setProperty(DEPLOYMENT_SERVICE, service_name[0])
      else
        raise "Ambiguous service names specified: #{service_name.join(', ')}"
      end
    rescue => e
      error("Argument parsing failed: #{e.to_s()}")
      return false
    end
    
    is_valid?()
  end
  
  def output_usage
    puts "Usage: configure-service [general-options] {-C|-D|-U} [service-options] service-name"
    output_general_usage()
    Configurator.instance.write_divider()
    puts "Service options:"
    output_usage_line("-C", "Create a replication service")
    output_usage_line("-D", "Delete a replication service")
    output_usage_line("-U", "Update a replication service")
    output_usage_line("--allow-bidi-unsafe [true|false]", "Allow unsafe SQL from remote services", "false")
    output_usage_line("--allow-any-remote-service [true|false]", "Replicate from any service", "false")
    output_usage_line("--auto-enable [true|false]", "If true, service goes online at startup", "true")
    output_usage_line("--buffer-size #", "Size of buffers for block commit and queues", "10")
    output_usage_line("--channels #", "Number of channels for parallel apply", "1")
    output_usage_line("--local-service-name name", "Replicator service that owns master")
    output_usage_line("--master-host host_name", "Replicator remote master host name")
    output_usage_line("--master-port #", "Replicator remote master THL port")
    output_usage_line("--role [master|slave|direct]", "Replicator role", "slave")
    output_usage_line("--service-type [local|remote]", "Replicator service type", "local")
    output_usage_line("--shard-default-db [stringent|relaxed]", "Use default db for shard ID", "stringent")
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
  
  def get_validation_checks
    []
  end
  
  def store_config_file?
    false
  end
end

module NotDeleteServicePrompt
  def enabled?
    super() && @config.getProperty(DEPLOYMENT_TYPE) != ConfigureServicePackage::SERVICE_DELETE
  end
end