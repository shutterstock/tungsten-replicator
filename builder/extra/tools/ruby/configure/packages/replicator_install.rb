class ReplicatorInstallPackage < ConfigurePackage
  def get_prompts
    ConfigurePackageCluster.new(@config).get_prompts() + [
    ]
  end
  
  def get_non_interactive_prompts
    [
    ]
  end
  
  def get_validation_checks
    ConfigurePackageCluster.new(@config).get_validation_checks() + [
    ]
  end
  
  def parsed_options?(arguments)
    reset_errors()
    @config.props = {}
    @config.setProperty(DEPLOY_CURRENT_PACKAGE, "true")
    @config.setProperty(DEPLOYMENT_TYPE, DISTRIBUTED_DEPLOYMENT_NAME)
    
    method = nil
    opts=OptionParser.new
    opts.on("--direct")         { method = "direct"
                                  @display_direct_help = true }
    opts.on("--master-slave")   { method = "master-slave"
                                  @display_ms_help = true }
    opts.on("--help-direct")    { @display_direct_help = true
                                  Configurator.instance.display_help?(true) }
    opts.on("--help-master-slave")  { @display_ms_help = true
                                  Configurator.instance.display_help?(true)}
    opts.on("--help-all")       { @display_direct_help = true
                                  @display_ms_help = true
                                  Configurator.instance.display_help?(true) }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments)

    case method
    when "direct"
      @display_direct_help = true
      parse_direct_arguments(remainder)
    when "master-slave"
      @display_ms_help = true
      parse_master_slave_arguments(remainder)
    else
      unless Configurator.instance.display_help?()
        error("You must specify either --direct or --master-slave")
      end
    end
    
    if Configurator.instance.display_help?()
      reset_errors()
    end
    
    is_valid?()
  end
  
  def parse_direct_arguments(arguments)
    opts = OptionParser.new
    host_options = Properties.new()
    host_options.setProperty(HOST, Configurator.instance.hostname())
    
    service_options = Properties.new()
    service_options.setProperty(DEPLOYMENT_HOST, DIRECT_DEPLOYMENT_HOST_ALIAS)
    service_options.setProperty(REPL_ROLE, REPL_ROLE_DI)
    
    datasource_options = Properties.new()
    datasource_options.setProperty([DATASOURCES, "master"], {})
    datasource_options.setProperty([DATASOURCES, "slave"], {})
    
    each_host_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          host_options.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          host_options.setProperty(prompt.name, val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    
    each_service_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          service_options.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          service_options.setProperty(prompt.name, val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    
    opts.on("--disable-relay-logs") {
      datasource_options.setProperty([DATASOURCES, "master", REPL_DISABLE_RELAY_LOGS], "true")
      datasource_options.setProperty([DATASOURCES, "slave", REPL_DISABLE_RELAY_LOGS], "true")
      ConfigurePrompt.add_global_default(REPL_DISABLE_RELAY_LOGS, "true")
    }
    each_datasource_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument().gsub('datasource', 'master')}") {
          datasource_options.setProperty([DATASOURCES, "master", prompt.name], av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument().gsub('datasource', 'master')} String") {
          |val|
          datasource_options.setProperty([DATASOURCES, "master", prompt.name], val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    
    each_datasource_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument().gsub('datasource', 'slave')}") {
          datasource_options.setProperty([DATASOURCES, "slave", prompt.name], av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument().gsub('datasource', 'slave')} String") {
          |val|
          datasource_options.setProperty([DATASOURCES, "slave", prompt.name], val)
        }
      end
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments, false, "invalid option for --direct")
    
    @config.setProperty(HOSTS, nil)
    @config.setProperty([HOSTS, DIRECT_DEPLOYMENT_HOST_ALIAS], 
      host_options.props.dup
    )
    
    @config.setProperty(DATASOURCES, {})
    master_alias = datasource_options.getProperty([DATASOURCES, "master", REPL_DBHOST]).gsub(".", "_") + "_" + datasource_options.getProperty([DATASOURCES, "master", REPL_DBPORT])
    slave_alias = datasource_options.getProperty([DATASOURCES, "slave", REPL_DBHOST]).gsub(".", "_") + "_" + datasource_options.getProperty([DATASOURCES, "slave", REPL_DBPORT])
    @config.setProperty([DATASOURCES, master_alias], datasource_options.getProperty([DATASOURCES, "master"]).dup())
    @config.setProperty([DATASOURCES, slave_alias], datasource_options.getProperty([DATASOURCES, "slave"]).dup())
    service_options.setProperty(REPL_DATASOURCE, slave_alias)
    service_options.setProperty(REPL_MASTER_DATASOURCE, master_alias)
    
    @config.setProperty(REPL_SERVICES, {})
    @config.setProperty([REPL_SERVICES, service_options.getProperty(DEPLOYMENT_SERVICE)], 
      service_options.props.dup
    )
    
    unless Configurator.instance.display_help?()
      unless datasource_options.getNestedProperty([DATASOURCES, "master", REPL_DBHOST])
        error("You must specify a value for --master-host")
      end
    
      unless datasource_options.getNestedProperty([DATASOURCES, "slave", REPL_DBHOST])
        error("You must specify a value for --slave-host")
      end
    
      unless service_options.getProperty(DEPLOYMENT_SERVICE)
        error("You must specify a value for --service-name")
      end
    end
  end
  
  def parse_master_slave_arguments(arguments)
    opts = OptionParser.new
    
    host_options = Properties.new()
    host_options.setProperty(HOST, Configurator.instance.hostname())
    
    service_options = Properties.new()
    service_options.setProperty(DEPLOYMENT_HOST, DIRECT_DEPLOYMENT_HOST_ALIAS)
    
    datasource_options = Properties.new()
    datasource_options.setProperty([DATASOURCES, "ds"], {})
    
    cluster_hosts = [Configurator.instance.hostname()]
    opts.on("--cluster-hosts String") {
      |val|
      cluster_hosts = val.split(',')
    }
    
    master_host = nil
    opts.on("--master-host String") {
      |val|
      master_host = val
    }
    
    each_host_prompt{
      |prompt|
      
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          host_options.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          host_options.setProperty(prompt.name, val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    
    {
      "master-thl-port" => REPL_MASTERPORT
    }.each{
      |arg, key|
      opts.on("--#{arg} String")  {
        |val| service_options.setProperty(key, val)
      }
    }
    
    each_service_prompt{
      |prompt|
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          service_options.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          service_options.setProperty(prompt.name, val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }

    opts.on("--disable-relay-logs") {
      datasource_options.setProperty([DATASOURCES, "ds", REPL_DISABLE_RELAY_LOGS], "true")
      ConfigurePrompt.add_global_default(REPL_DISABLE_RELAY_LOGS, "true")
    }
    each_datasource_prompt{
      |prompt|
      if prompt.is_a?(DatasourceDBHost)
        next
      end
      
      if (av = prompt.get_command_line_argument_value()) != nil
        opts.on("--#{prompt.get_command_line_argument()}") {
          datasource_options.setProperty(prompt.name, av)
          ConfigurePrompt.add_global_default(prompt.name, av)
        }
      else
        opts.on("--#{prompt.get_command_line_argument()} String") {
          |val|
          datasource_options.setProperty([DATASOURCES, "ds", prompt.name], val)
          ConfigurePrompt.add_global_default(prompt.name, val)
        }
      end
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments, false, "invalid option for --master-slave")
    
    cluster_hosts.each{
      |host|
      host_alias = host.tr('.', '_')
      @config.setProperty([HOSTS, host_alias], host_options.props.dup)
      @config.setProperty([HOSTS, host_alias, HOST], host)
      
      datasource_alias = host_alias
      @config.setProperty([DATASOURCES, datasource_alias],
        datasource_options.getProperty([DATASOURCES, "ds"]).dup)
      @config.setProperty([DATASOURCES, datasource_alias, REPL_DBHOST], host)
      
      service_alias = service_options.getProperty(DEPLOYMENT_SERVICE) + "_" + host_alias
      @config.setProperty([REPL_SERVICES, service_alias], service_options.props.dup)
      @config.setProperty([REPL_SERVICES, service_alias, DEPLOYMENT_HOST],
        host_alias)
      @config.setProperty([REPL_SERVICES, service_alias, REPL_DATASOURCE],
        datasource_alias)
      
      if host == master_host &&
          (@config.getProperty([REPL_SERVICES, service_alias, REPL_MASTERPORT]) == 
          @config.getProperty([REPL_SERVICES, service_alias, REPL_SVC_THL_PORT]))
        @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "master")
      else
        @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "slave")
        @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERHOST], master_host)
      end
    }
    
    unless Configurator.instance.display_help?()
      if master_host == nil
        error("You must specify a value for --master-host")
      end
    
      unless cluster_hosts.include?(master_host)
        confirm("The master-host (#{master_host}) does not appear in the cluster-hosts (#{cluster_hosts.join(', ')}).")
      end
    
      if service_options.getProperty(DEPLOYMENT_SERVICE) == nil
        error("You must specify a value for --service-name")
      end
    end
  end
  
  def output_usage()
    ph = ConfigurePromptHandler.new(@config)
    puts "Usage: tungsten-installer [general-options] {--direct|--master-slave} [--help-direct|--help-master-slave|--help-all] [install-options]"
    output_general_usage()
    
    if @display_direct_help
      Configurator.instance.write_divider(Logger::ERROR)
      puts "Install options: --direct"
      each_host_prompt{
        |prompt|
        prompt.output_usage()
      }
      
      output_usage_line("--disable-relay-logs", "Disable the use of relay-logs?")
      each_datasource_prompt{
        |prompt|
        output_usage_line("--#{prompt.get_command_line_argument()}".gsub("datasource", "master"), prompt.get_prompt(), prompt.get_value(true, true), nil, prompt.get_prompt_description())
      }
      
      each_datasource_prompt{
        |prompt|
        output_usage_line("--#{prompt.get_command_line_argument()}".gsub("datasource", "slave"), prompt.get_prompt(), prompt.get_value(true, true), nil, prompt.get_prompt_description())
      }
      
      each_service_prompt{
        |prompt|
        prompt.output_usage()
      }
    end
    
    if @display_ms_help
      Configurator.instance.write_divider(Logger::ERROR)
      puts "Install options: --master-slave"
      output_usage_line("--cluster-hosts")
      
      prompt = ph.find_prompt(ReplicationServiceTHLMaster)
      output_usage_line("--master-host", prompt.get_prompt(), prompt.get_value(true, true), nil, prompt.get_prompt_description())
      
      each_host_prompt{
        |prompt|
        prompt.output_usage()
      }
      
      output_usage_line("--disable-relay-logs", "Disable the use of relay-logs?")
      each_datasource_prompt{
        |prompt|
        if prompt.is_a?(DatasourceDBHost)
          next
        end
        
        prompt.output_usage()
      }
      
      output_usage_line("--master-log-file", "PENDING")
      output_usage_line("--master-log-pos", "PENDING")
      prompt = ph.find_prompt(ReplicationServiceTHLMasterPort)
      prompt.output_usage()
      
      each_service_prompt{
        |prompt|
        prompt.output_usage()
      }
    end
  end
end

module NotTungstenInstallerPrompt
  def enabled_for_command_line?
    unless Configurator.instance.package.is_a?(ReplicatorInstallPackage)
      super() && true
    else
      false
    end
  end
end