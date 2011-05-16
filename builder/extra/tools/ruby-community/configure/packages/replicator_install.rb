class ReplicatorInstallPackage < ConfigurePackage
  def get_prompts
    ConfigurePackageCluster.new(@config).get_prompts() + [
      ReplicationServices.new()
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
    
    if Configurator.instance.display_help?()
      output_usage()
      exit 0
    end
    
    unless @config.getProperty(DEPLOYMENT_TYPE)
      error("You must specify either --direct or --master-slave")
    end
    
    unless is_valid?
      raise "There are issues with the command options"
    end

    case method
    when "direct"
      @display_direct_help = true
      options = parse_direct_arguments(remainder)
      unless is_valid?
        return false
      end
      
      process_direct_options(options)
      unless is_valid?
        return false
      end
    when "master-slave"
      @display_ms_help = true
      options = parse_master_slave_arguments(remainder)
      unless is_valid?
        return false
      end
      
      process_master_slave_options(options)
      unless is_valid?
        return false
      end
    else
      raise "Invalid deployment type specified"
    end
    
    Configurator.instance.save_prompts()
    
    true
  end
  
  def parse_direct_arguments(arguments)
    options = Properties.new()
    options.props = {
      "host" => Configurator.instance.hostname(),
      "master-port" => "3306",
      "master-user" => Configurator.instance.whoami(),
      "slave-port" => "3306",
      "slave-user" => Configurator.instance.whoami(),
      "thl-mode" => "disk",
      "thl-port" => "2112",
      "rmi-port" => "10000",
      "svc-start" => "false",
      "report-services" => "false",
      "home-directory" => Configurator.instance.get_base_path()
    }
    
    opts = OptionParser.new
    opts.on("--start")      {options.setProperty("svc-start", "true")}
    opts.on("--start-and-report")      {
      options.setProperty("svc-start", "true")
      options.setProperty("svc-report", "true")
    }
    
    [
      "dbms-type",
      "home-directory",
      "host",
      "master-alias",
      "master-host",
      "master-port",
      "master-user",
      "master-password",
      "master-log-directory",
      "master-log-pattern",
      "master-log-file",
      "master-log-pos",
      "slave-alias",
      "slave-host",
      "slave-port",
      "slave-user",
      "slave-password",
      "thl-directory",
      "thl-port",
      "relay-directory",
      "buffer-size",
      "channels",
      "service-name",
      "rmi-port",
      "user",
    ].each{
      |prop_key|
      opts.on("--#{prop_key} String")  {|val| options.setProperty(prop_key, val)}
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments, false, "invalid option for --direct")
    
    options
  end
  
  def process_direct_options(options)
    unless options.getProperty("master-host")
      error("You must specify a value for --master-host")
    end
    
    unless options.getProperty("slave-host")
      error("You must specify a value for --slave-host")
    end
    
    unless options.getProperty("service-name")
      error("You must specify a value for --service-name")
    end
    
    unless is_valid?
      raise "There are issues with the command options"
    end
    
    master_alias = options.getPropertyOr("master-alias", options.getProperty("master-host").tr(".", "_")) + "_" + options.getPropertyOr("master-port", "3306")
    slave_alias = options.getPropertyOr("slave-alias", options.getProperty("slave-host").tr(".", "_")) + "_" + options.getPropertyOr("slave-port", "3306")
    
    if options.getProperty("home-directory") == Configurator.instance.get_base_path()
      options.setProperty("home-directory", Configurator.instance.get_base_path())
      options.setProperty("current-release-directory", Configurator.instance.get_base_path())
    end
    
    @config.setProperty(DBMS_TYPE, options.getProperty("dbms-type"))
    @config.setProperty(DEPLOYMENT_HOST, DIRECT_DEPLOYMENT_HOST_ALIAS)
    
    @config.setProperty(HOSTS, nil)
    @config.setProperty([HOSTS, DIRECT_DEPLOYMENT_HOST_ALIAS], {
      SVC_START => options.getProperty('svc-start'),
      SVC_REPORT => options.getProperty('svc-report'),
      HOST => options.getProperty("host"),
      USERID => options.getProperty("user"),
      HOME_DIRECTORY => options.getProperty("home-directory"),
      CURRENT_RELEASE_DIRECTORY => options.getProperty("current-release-directory"),
      REPL_LOG_DIR => options.getPropertyOr("thl-directory", options.getProperty("home-directory") + "/thl"),
      REPL_RELAY_LOG_DIR => options.getPropertyOr("relay-directory", options.getProperty("home-directory") + "/relay"),
      REPL_RMI_PORT => options.getProperty("rmi-port")
    })
    
    @config.setProperty(DATASERVERS, nil)
    @config.setProperty([DATASERVERS, master_alias], {
      REPL_DBHOST => options.getProperty("master-host"),
      REPL_DBPORT => options.getProperty("master-port"),
      REPL_DBLOGIN => options.getProperty("master-user"),
      REPL_DBPASSWORD => options.getProperty("master-password"),
      REPL_MYSQL_BINLOGDIR => options.getProperty("master-log-directory"),
      REPL_MYSQL_BINLOGPATTERN => options.getProperty("master-log-pattern")
    })
    @config.setProperty([DATASERVERS, slave_alias], {
      REPL_DBHOST => options.getProperty("slave-host"),
      REPL_DBPORT => options.getProperty("slave-port"),
      REPL_DBLOGIN => options.getProperty("slave-user"),
      REPL_DBPASSWORD => options.getProperty("slave-password"),
    })
    
    @config.setProperty(REPL_SERVICES, {})
    @config.setProperty([REPL_SERVICES, options.getProperty("service-name")], {
      DEPLOYMENT_HOST => DIRECT_DEPLOYMENT_HOST_ALIAS,
      DSNAME => options.getProperty("service-name"),
      DEPLOYMENT_SERVICE => options.getProperty("service-name"),
      REPL_ROLE => "direct",
      REPL_EXTRACTOR_DATASERVER => master_alias,
      REPL_DATASERVER => slave_alias,
      REPL_BUFFER_SIZE => options.getProperty("buffer-size"),
      REPL_SVC_CHANNELS => options.getProperty("channels"),
      REPL_SVC_THL_PORT => options.getProperty("thl-port")
    })
  end
  
  def parse_master_slave_arguments(arguments)
    options = Properties.new()
    options.props = {
      "datasource-port" => "3306",
      "datasource-user" => Configurator.instance.whoami(),
      "thl-port" => "2112",
      "rmi-port" => "10000",
      "svc-start" => "false",
      "svc-report" => "false",
      "home-directory" => Configurator.instance.get_base_path()
    }
    
    opts = OptionParser.new
    opts.on("--start")      {options.setProperty("svc-start", "true")}
    opts.on("--start-and-report")      {
      options.setProperty("svc-start", "true")
      options.setProperty("svc-report", "true")
    }

    [
      "dbms-type",
      "cluster-hosts",
      "master-host",
      "user",
      "home-directory",
      "datasource-port",
      "datasource-user",
      "datasource-password",
      "datasource-log-directory",
      "datasource-log-pattern",
      "master-log-file",
      "master-log-pos",
      "datasource-transfer-logs",
      "thl-directory",
      "thl-port",
      "relay-directory",
      "buffer-size",
      "channels",
      "service-name",
      "rmi-port"
    ].each{
      |prop_key|
      opts.on("--#{prop_key} String")  {|val| options.setProperty(prop_key, val)}
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments, false, "invalid option for --master-slave")
    
    options
  end
  
  def process_master_slave_options(options)
    if options.getProperty("cluster-hosts") == nil
      error("You must specify cluster-hosts")
    end
    
    if options.getProperty("master-host") == nil
      error("You must specify a master-host")
    end
    
    if options.getProperty("service-name") == nil
      error("You must specify a service-name")
    end
    
    unless is_valid?
      raise "There are issues with the command options"
    end
    
    if options.getProperty("home-directory") == Configurator.instance.get_base_path()
      options.setProperty("home-directory", Configurator.instance.get_base_path())
      options.setProperty("current-release-directory", Configurator.instance.get_base_path())
    end
    
    @config.setProperty(DBMS_TYPE, options.getProperty("dbms-type"))
    
    @config.setProperty(HOSTS, nil)
    @config.setProperty(DATASERVERS, nil)
    @config.setProperty(REPL_SERVICES, nil)
    
    options.getProperty("cluster-hosts").split(',').each{
      |host|
      host_alias = host.tr('.', '_')
      @config.setProperty([HOSTS, host_alias], {
        SVC_START => options.getProperty('svc-start'),
        SVC_REPORT => options.getProperty('svc-report'),
        HOST => host,
        USERID => options.getProperty("user"),
        HOME_DIRECTORY => options.getProperty("home-directory"),
        CURRENT_RELEASE_DIRECTORY => options.getProperty("current-release-directory"),
        REPL_LOG_DIR => options.getProperty("thl-directory"),
        REPL_RELAY_LOG_DIR => options.getProperty("relay-directory"),
        REPL_RMI_PORT => options.getProperty("rmi-port")
      })
      
      @config.setProperty([DATASERVERS, host_alias], {
        REPL_DBHOST => host,
        REPL_DBPORT => options.getProperty("datasource-port"),
        REPL_DBLOGIN => options.getProperty("datasource-user"),
        REPL_DBPASSWORD => options.getProperty("datasource-password")
      })
      
      service_alias = options.getProperty("service-name") + "_" + host_alias
      @config.setProperty([REPL_SERVICES, service_alias], {
        DEPLOYMENT_HOST => host_alias,
        DSNAME => options.getProperty("service-name"),
        DEPLOYMENT_SERVICE => options.getProperty("service-name"),
        REPL_DATASERVER => host_alias,
        REPL_BUFFER_SIZE => options.getProperty("buffer-size"),
        REPL_SVC_CHANNELS => options.getProperty("channels"),
        REPL_SVC_THL_PORT => options.getProperty("thl-port")
      })
      
      if host == options.getProperty("master-host")
        @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "master")
      else
        @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "slave")
        @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERHOST], options.getProperty("master-host"))
        @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERPORT], options.getProperty("thl-port"))
      end
    }
  end
  
  def post_prompt_handler_run
    Configurator.instance.save_prompts()
  end
  
  def output_usage()
    puts "Usage: tungsten-installer [general-options] {--direct|--master-slave} [--help-direct|--help-master-slave|--help-all] [install-options]"
    output_general_usage()

    if @display_direct_help
      Configurator.instance.write_divider(Logger::ERROR)
      puts "Install options: --direct"
      output_usage_line("--dbms-type [mysql|postgresql]", "", "mysql")
      output_usage_line("--home-directory")
      output_usage_line("--master-alias")
      output_usage_line("--master-host")
      output_usage_line("--master-port", "", "3306")
      output_usage_line("--master-user", "", Configurator.instance.whoami())
      output_usage_line("--master-password")
      output_usage_line("--master-log-directory", "", "/var/lib/mysql")
      output_usage_line("--master-log-pattern", "", "mysql-bin")
      output_usage_line("--master-log-file", "PENDING")
      output_usage_line("--master-log-pos", "PENDING")
      output_usage_line("--slave-alias")
      output_usage_line("--slave-host", "", Configurator.instance.hostname())
      output_usage_line("--slave-port", "", "3306")
      output_usage_line("--slave-user", "", Configurator.instance.whoami())
      output_usage_line("--slave-password")
      output_usage_line("--thl-directory", "", Configurator.instance.get_base_path() + "/thl")
      output_usage_line("--thl-port", "", "2112")
      output_usage_line("--relay-directory", "", Configurator.instance.get_base_path() + "/relay")
      output_usage_line("--buffer-size", "Size of buffers for block commit and queues", "10")
      output_usage_line("--channels", "Number of channels for parallel apply", "1")
      output_usage_line("--rmi-port", "", "10001")
      output_usage_line("--service-name")
      output_usage_line("--start", "Start the replicator after configuration")
      output_usage_line("--start-and-report", "Start the replicator and report out the services list after configuration")
    end
    
    if @display_ms_help
      Configurator.instance.write_divider(Logger::ERROR)
      puts "Install options: --master-slave"
      output_usage_line("--dbms-type [mysql|postgresql]", "", "mysql")
      output_usage_line("--cluster-hosts")
      output_usage_line("--master-host")
      output_usage_line("--user")
      output_usage_line("--home-directory", "", Configurator.instance.get_base_path())
      output_usage_line("--datasource-port", "", "3306")
      output_usage_line("--datasource-user", "", Configurator.instance.whoami())
      output_usage_line("--datasource-password")
      output_usage_line("--datasource-log-directory", "", "/var/lib/mysql")
      output_usage_line("--datasource-log-pattern", "", "mysql-bin")
      output_usage_line("--master-log-file", "PENDING")
      output_usage_line("--master-log-pos", "PENDING")
      output_usage_line("--datasource-transfer-logs")
      output_usage_line("--thl-directory", "", Configurator.instance.get_base_path() + "/thl")
      output_usage_line("--thl-port", "", "2112")
      output_usage_line("--relay-directory", "", Configurator.instance.get_base_path() + "/relay")
      output_usage_line("--buffer-size", "Size of buffers for block commit and queues", "10")
      output_usage_line("--channels", "Number of channels for parallel apply", "1")
      output_usage_line("--rmi-port", "", "10001")
      output_usage_line("--service-name")
      output_usage_line("--start", "Start the replicator after configuration")
      output_usage_line("--start-and-report", "Start the replicator and report out the services list after configuration")
    end
  end
  
  def store_config_file?
    false
  end
end