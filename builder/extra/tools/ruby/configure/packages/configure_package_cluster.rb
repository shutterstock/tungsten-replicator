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
    
    reset_errors()
    
    if @config.props.size > 0
      error("Unable to run configure because this directory is already configured")
      return false
    end
    
    cluster_hosts = [Configurator.instance.hostname()]
    host_config = Properties.new()
    
    opts = OptionParser.new    
    opts.on("--cluster-hosts String")  {|val| cluster_hosts = val.split(",")}
    each_host_prompt{
      |prompt|
      opts.on("--#{prompt.get_command_line_argument()} String") {
        |val|
        host_config.setProperty(prompt.name, val)
      }
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    if host_config.getProperty("home-directory") == Configurator.instance.get_base_path()
      host_config.setProperty("home-directory", Configurator.instance.get_base_path())
      host_config.setProperty("current-release-directory", Configurator.instance.get_base_path())
    end
    
    cluster_hosts.each{
      |host|
      host_alias = host.tr('.', '_')
      
      host_config.setProperty(HOST, host)
      @config.setProperty([HOSTS, host_alias], host_config.props)
    }
    
    is_valid?()
  end
  
  def output_usage()
    host = @config.getPropertyOr(HOSTS, {}).keys.at(0)
    
    puts "Usage: configure [general-options] [install-options]"
    output_general_usage()
    
    Configurator.instance.write_divider(Logger::ERROR)
    puts "Install options:"
    
    output_usage_line("--cluster-hosts", "The hosts to install Tungsten Replicator to", Configurator.instance.hostname())
    
    each_host_prompt{
      |prompt|
      prompt.set_member(host || DEFAULTS)
      prompt.output_usage()
    }
  end
  
  def store_config_file?
    Configurator.instance.is_interactive?()
  end
end