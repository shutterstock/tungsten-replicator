TEMP_DEPLOYMENT_SERVICE = "temp"

class ConfigureServicePackage < ConfigurePackage
  SERVICE_CREATE = "create_service"
  SERVICE_DELETE = "delete_service"
  SERVICE_UPDATE = "update_service"
  
  def parsed_options?(arguments)
    @config.setProperty(DEPLOYMENT_TYPE, nil)
    @config.setProperty(DEPLOY_CURRENT_PACKAGE, nil)
    @config.setProperty(DEPLOY_PACKAGE_URI, nil)
    
    service_config = Properties.new
    
    opts=OptionParser.new
    
    opts.on("-C", "--create")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_CREATE) }
    opts.on("-D", "--delete")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_DELETE) }
    opts.on("-U", "--update")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_UPDATE) }
    opts.on("--start")          { service_config.setProperty(REPL_SVC_START, "true") }
    
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
      opts.on("#{arg} String")  {|val|  service_config.setProperty(prop_key, val) }
    }
    
    begin
      remainder = Configurator.instance.run_option_parser(opts, arguments)
      
      if Configurator.instance.display_help?()
        output_usage()
        exit 0
      end
      
      unless @config.getProperty(DEPLOYMENT_TYPE)
        error("You must specify -C, -D or -U")
      end
      
      case remainder.size()
      when 0
        raise "No service_name specified"
      when 1
        deploy_service_key = false
        @config.getPropertyOr(REPL_SERVICES, {}).each_key{
          |s_key|
          if @config.getProperty([REPL_SERVICES, s_key, DEPLOYMENT_SERVICE]) == remainder[0]
            deploy_service_key = s_key
          end
        }
        
        case @config.getProperty(DEPLOYMENT_TYPE)
        when SERVICE_CREATE
          if deploy_service_key != false
            raise "A service named '#{remainder[0]}' already exists"
          else
            deploy_service_key = remainder[0]
            service_config.setProperty(DEPLOYMENT_SERVICE, deploy_service_key)
            @config.setProperty([REPL_SERVICES, remainder[0]], service_config.props)
          end
        when SERVICE_UPDATE
          if deploy_service_key == false
            raise "Unable to find an existing service config for '#{remainder[0]}'"
          else
            service_config.props.each{
              |sc_key, sc_val|
              @config.setProperty([REPL_SERVICES, deploy_service_key, sc_key], sc_val)
            }
          end
        when SERVICE_DELETE
          if deploy_service_key == false
            raise "Unable to find an existing service config for '#{remainder[0]}'"
          end
        end
      else
        raise "Multiple service names specified: #{remainder.join(', ')}"
      end
    rescue => e
      error("Argument parsing failed: #{e.to_s()}")
      return false
    end
    
    @config.setProperty(DEPLOYMENT_SERVICE, deploy_service_key)
    
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
    output_usage_line("--datasource alias", "Configuration alias of the datasource to use when applying events")
    output_usage_line("--service-type [local|remote]", "Replicator service type", "local")
    output_usage_line("--shard-default-db [stringent|relaxed]", "Use default db for shard ID", "stringent")
  end
  
  def get_prompts
    [
      ReplicationServices.new(),
      DeploymentServicePrompt.new()
    ]
  end
  
  def get_non_interactive_prompts
    cluster_prompts = ConfigurePackageCluster.new(@config).get_prompts()
    cluster_prompts.delete_if{ |prompt| prompt.is_a?(ReplicationServices)}
    
    cluster_prompts
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