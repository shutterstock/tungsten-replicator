class ConfigurePackageCluster < ConfigurePackage
  def get_prompts
    datasources = Datasources.new
    datasources.extend(AdvancedPromptModule)
    
    services = ReplicationServices.new()
    services.extend(AdvancedPromptModule)
    
    [
      DeploymentTypePrompt.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      ClusterNamePrompt.new(),
      DeploymentHost.new(),
      ClusterHosts.new(),
      datasources,
      services
    ]
  end
  
  def get_validation_checks
    checks = []
    
    ClusterHostCheck.subclasses.each{
      |klass|
      checks << klass.new()
    }
    
    return checks
  end
  
  def parsed_options?(arguments)
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?()
      return true
    end
    
    unless Configurator.instance.display_preview?
      if @config.props.size > 0
        error("Unable to run configure because this directory is already setup")
        return false
      end
    end
    
    cluster_hosts = nil
    host_options = Properties.new()
    
    opts = OptionParser.new    
    opts.on("--cluster-hosts String")  {|val| cluster_hosts = val.split(",")}
    each_host_prompt{
      |prompt|
      opts.on("--#{prompt.get_command_line_argument()} String") {
        |val|
        host_options.setProperty(prompt.name, val)
      }
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    unless Configurator.instance.display_preview?
      if cluster_hosts == nil && host_options.props.size == 0
        confirm("This process will configure the current directory to run the replicator with all defaults.  You will need to run tools/configure-service to add replication services.")
      end
    end
    
    if cluster_hosts == nil
      cluster_hosts = [Configurator.instance.hostname()]
    end
    
    if host_options.getProperty("home-directory") == Configurator.instance.get_base_path()
      host_options.setProperty("home-directory", Configurator.instance.get_base_path())
      host_options.setProperty("current-release-directory", Configurator.instance.get_base_path())
    end
    
    host_options.setProperty(FIXED_PROPERTY_STRINGS, Configurator.instance.fixed_properties)
    
    cluster_hosts.each{
      |host|
      host_alias = host.tr('.', '_')
      
      host_options.setProperty(HOST, host)
      @config.setProperty([HOSTS, host_alias], host_options.props)
    }
    
    is_valid?()
  end
  
  def output_usage()
    host_alias = @config.getPropertyOr(HOSTS, {}).keys.at(0)
    
    puts "Usage: configure [general-options] [install-options]"
    output_general_usage()
    
    Configurator.instance.write_divider(Logger::ERROR)
    puts "Install options:"
    
    output_usage_line("--cluster-hosts", "The hosts to install Tungsten Replicator to", Configurator.instance.hostname())
    
    each_host_prompt{
      |prompt|
      prompt.set_member(host_alias || DEFAULTS)
      prompt.output_usage()
    }
  end
  
  def store_config_file?
    Configurator.instance.is_interactive?()
  end
end