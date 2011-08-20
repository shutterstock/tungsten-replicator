TEMP_DEPLOYMENT_SERVICE = "temp"

class ConfigureServicePackage < ConfigurePackage
  SERVICE_CREATE = "create_service"
  SERVICE_DELETE = "delete_service"
  SERVICE_UPDATE = "update_service"
  
  def parsed_options?(arguments)
    deployment_host = @config.getNestedProperty([DEPLOYMENT_HOST])
    if deployment_host.to_s == ""
      deployment_host = DEFAULTS
    end
    
    if deployment_host == DEFAULTS
      @target_host = Configurator.instance.hostname
    else
      @target_host = @config.getProperty([HOSTS, deployment_host, HOST])
    end
    @target_user = @config.getProperty([HOSTS, deployment_host, USERID])
    @target_home_directory = @config.getProperty([HOSTS, deployment_host, CURRENT_RELEASE_DIRECTORY])
    @load_remote_config = false
    
    opts=OptionParser.new
    opts.on("--host String")    { |val| 
      @load_remote_config = true
      @target_host = val }
    opts.on("--user String")    { |val| 
      @target_user = val }
    opts.on("--release-directory String")  { |val| 
      @load_remote_config = true
      @target_home_directory = val }
    
    arguments = Configurator.instance.run_option_parser(opts, arguments)

    if @load_remote_config == true
      info "Load the current config from #{@target_user}@#{@target_host}:#{@target_home_directory}"
      
      begin
        command = "cd #{@target_home_directory}; tools/configure --output-config"    
        config_output = ssh_result(command, @target_host, @target_user)
        parsed_contents = JSON.parse(config_output)
        unless parsed_contents.instance_of?(Hash)
          raise "invalid object"
        end
        @config.props = parsed_contents.dup
      rescue
        raise "Unable to load the current config from #{@target_user}@#{@target_host}:#{@target_home_directory}"
      end
    end
    
    @config.setProperty(DEPLOYMENT_TYPE, nil)
    @config.setProperty(DEPLOY_CURRENT_PACKAGE, nil)
    @config.setProperty(DEPLOY_PACKAGE_URI, nil)
    
    service_config = Properties.new
    
    opts=OptionParser.new
    
    opts.on("-C", "--create")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_CREATE) }
    opts.on("-D", "--delete")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_DELETE) }
    opts.on("-U", "--update")   { @config.setProperty(DEPLOYMENT_TYPE, SERVICE_UPDATE) }
    
    each_service_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          service_config.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          service_config.setProperty(prompt.name, val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    opts.on("--master-host String") {
      |val|
      service_config.setProperty(REPL_MASTERHOST, val)
      ConfigurePrompt.add_global_default(REPL_MASTERHOST, val)
      warning("--master-host is deprecated, use --master-thl-host instead.")
    }
    
    begin
      remainder = Configurator.instance.run_option_parser(opts, arguments)
      
      unless @config.getNestedProperty([DEPLOYMENT_TYPE])
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
    end
    
    @config.setProperty(DEPLOYMENT_SERVICE, deploy_service_key)
    
    if Configurator.instance.display_help?()
      svc = @config.getProperty(DEPLOYMENT_SERVICE)
      if svc == ""
        svc = DEFAULTS
      end
      service_config.props.each{
        |sc_key, sc_val|
        @config.setProperty([REPL_SERVICES, svc, sc_key], sc_val)
      }
      @config.setDefault([REPL_SERVICES, svc, DEPLOYMENT_HOST], DEFAULTS)
      
      reset_errors()
    end
    
    is_valid?()
  end
  
  def output_usage
    puts "Usage: configure-service [general-options] {-C|-D|-U} [target-options] [service-options] service-name"
    output_general_usage()
    
    Configurator.instance.write_divider()
    puts "Target options:"
    output_usage_line("--host", "Host to connect to configure the service", @target_host)
    output_usage_line("--user", "User to connect to the host as", @target_user)
    output_usage_line("--release-directory", "The release directory that holds the Tungsten runtime files", @target_home_directory)
    
    Configurator.instance.write_divider()
    puts "Service options:"
    output_usage_line("-C", "Create a replication service")
    output_usage_line("-D", "Delete a replication service")
    output_usage_line("-U", "Update a replication service")
    
    svc = @config.getProperty(DEPLOYMENT_SERVICE)
    if svc == ""
      svc = DEFAULTS
    end
    
    each_service_prompt{
      |prompt|
      prompt.set_member(svc)
      prompt.output_usage()
    }
  end
  
  def get_prompts
    [
      Datasources.new(),
      ReplicationServices.new(),
      DeploymentServicePrompt.new()
    ]
  end
  
  def get_non_interactive_prompts
    cluster_prompts = ConfigurePackageCluster.new(@config).get_prompts()
    cluster_prompts.delete_if{ |prompt| 
      (prompt.is_a?(ReplicationServices) || prompt.is_a?(Datasources))
    }
    
    cluster_prompts
  end
  
  def get_validation_checks
    [
      ReplicationServiceChecks.new()
    ]
  end
  
  def each_service_prompt(&block)
    ch = ReplicationServices.new()
    ch.set_config(@config)
    
    ch.each_prompt{
      |prompt|
      
      if prompt.enabled_for_command_line?()
        begin
          block.call(prompt)
        rescue => e
          error(e.message)
        end
      end
    }
  end
  
  def read_config_file?
    true
  end
  
  def allow_interactive?
    false
  end
  
  def allow_batch?
    false
  end
end

module NotDeleteServicePrompt
  def required?
    super() && @config.getProperty(DEPLOYMENT_TYPE) != ConfigureServicePackage::SERVICE_DELETE
  end
end