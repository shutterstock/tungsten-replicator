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
    
    opts=OptionParser.new
    opts.on("--direct")         { @config.setProperty(DEPLOYMENT_TYPE, DIRECT_DEPLOYMENT_NAME) }
    opts.on("--master-slave")   { @config.setProperty(DEPLOYMENT_TYPE, DISTRIBUTED_DEPLOYMENT_NAME) }
    
    begin
      opts.order!(arguments)
    rescue OptionParser::InvalidOption => io
      # Prepend the invalid option onto the arguments array
      arguments = io.recover(arguments)
    rescue => e
      error("Argument parsing failed: #{e.to_s()}")
      return false
    end
    
    unless @config.getProperty(DEPLOYMENT_TYPE)
      error("You must specify either --direct or --master-slave")
    end
    
    unless is_valid?
      raise "There are issues with the command options"
    end

    case @config.getProperty(DEPLOYMENT_TYPE)
    when DIRECT_DEPLOYMENT_NAME
      options = parse_direct_arguments(arguments)
      unless is_valid?
        return false
      end
      
      process_direct_options(options)
      unless is_valid?
        return false
      end
    when DISTRIBUTED_DEPLOYMENT_NAME
      options = parse_master_slave_arguments(arguments)
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
      "master-port" => "3306",
      "master-user" => Configurator.instance.whoami(),
      "slave-port" => "3306",
      "slave-user" => Configurator.instance.whoami(),
      "slave-thl-mode" => "disk",
      "slave-thl-directory" => "/opt/continuent/thl",
      "slave-relay-directory" => "/opt/continuent/relay",
      "slave-thl-port" => "2112",
      "rmi-port" => 10001
    }
    
    opts = OptionParser.new
    opts.on("--start")      {options.setProperty(REPL_SVC_START, "true")}

    [
      "dbms-type",
      "master-alias",
      "master-host",
      "master-port",
      "master-user",
      "master-password",
      "master-log-file",
      "master-log-pos",
      "slave-alias",
      "slave-host",
      "slave-port",
      "slave-user",
      "slave-password",
      "slave-thl-directory",
      "slave-thl-port",
      "slave-relay-directory",
      "buffer-size",
      "channels",
      "service-name",
      "rmi-port"
    ].each{
      |prop_key|
      opts.on("--#{prop_key} String")  {|val| options.setProperty(prop_key, val)}
    }
    
    begin
      opts.order!(arguments)
    rescue => e
      error("Argument parsing failed: #{e.to_s()}")
    end
    
    options
  end
  
  def process_direct_options(options)
    unless options.getProperty("master-host")
      error("You must specify a value for --master-host")
    end
    
    unless options.getProperty("slave-host")
      error("You must specify a value for --slave-host")
    end
    
    unless is_valid?
      raise "There are issues with the command options"
    end
    
    master_alias = options.getPropertyOr("master-alias", options.getProperty("master-host").tr(".", "_")) + "_" + options.getPropertyOr("master-port", "3306")
    slave_alias = options.getPropertyOr("slave-alias", options.getProperty("slave-host").tr(".", "_")) + "_" + options.getPropertyOr("slave-port", "3306")
    
    @config.setProperty(DBMS_TYPE, options.getProperty("dbms-type"))
    @config.setProperty(DEPLOYMENT_HOST, DIRECT_DEPLOYMENT_HOST_ALIAS)
    
    @config.setProperty(HOSTS, nil)
    @config.setProperty([HOSTS, DIRECT_DEPLOYMENT_HOST_ALIAS], {
      SVC_START => "false",
      HOST => Configurator.instance.hostname(),
      IP_ADDRESS => Resolv.getaddress(Configurator.instance.hostname()),
      REPL_LOG_DIR => options.getProperty("slave-thl-directory"),
      REPL_RELAY_LOG_DIR => options.getProperty("slave-relay-directory"),
      REPL_RMI_PORT => options.getProperty("rmi-port")
    })
    
    @config.setProperty(DATASERVERS, nil)
    @config.setProperty([DATASERVERS, master_alias], {
      REPL_DBHOST => options.getProperty("master-host"),
      REPL_DBPORT => options.getProperty("master-port"),
      REPL_DBLOGIN => options.getProperty("master-user"),
      REPL_DBPASSWORD => options.getProperty("master-password"),
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
      REPL_SVC_THL_PORT => options.getProperty("slave-thl-port")
    })
  end
  
  def parse_master_slave_arguments(arguments)
    options = Properties.new()
    options.props = {
      "datasource-port" => "3306",
      "datasource-user" => Configurator.instance.whoami(),
      "home-directory" => "/opt/continuent",
      "thl-directory" => "/opt/continuent/thl",
      "thl-port" => "2112",
      "relay-directory" => "/opt/continuent/relay",
      "rmi-port" => 10001
    }
    
    opts = OptionParser.new
    opts.on("--start")      {options.setProperty(REPL_SVC_START, "true")}

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
    
    begin
      opts.order!(arguments)
    rescue => e
      error("Argument parsing failed: #{e.to_s()}")
    end
    
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
    
    @config.setProperty(DBMS_TYPE, options.getProperty("dbms-type"))
    
    @config.setProperty(HOSTS, nil)
    @config.setProperty(DATASERVERS, nil)
    @config.setProperty(REPL_SERVICES, nil)
    
    options.getProperty("cluster-hosts").split(',').each{
      |host|
      host_alias = host.tr('.', '_')
      @config.setProperty([HOSTS, host_alias], {
        SVC_START => "false",
        HOST => host,
        IP_ADDRESS => Resolv.getaddress(host),
        USERID => options.getProperty("user"),
        HOME_DIRECTORY => options.getProperty("home-directory"),
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
        @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERHOST], options.getProperty("master-host").tr(".", "_"))
        @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERPORT], options.getProperty("thl-port"))
      end
    }
  end
  
  def post_prompt_handler_run
    Configurator.instance.save_prompts()
  end
  
  def output_usage
    puts "Usage: tungsten-installer [general-options] {--direct|--master-slave} [install-options]"
    output_general_usage()
    Configurator.instance.write_divider()
    puts "Install options: --direct"
    puts "--dbms-type                 (mysql|postgresql) [mysql]"
    puts "--master-alias"
    puts "--master-host"
    puts "--master-port               [3306]"
    puts "--master-user               [#{Configurator.instance.whoami()}]"
    puts "--master-password"
    puts "--master-log-file           PENDING"
    puts "--master-log-pos            PENDING"
    puts "--slave-alias"
    puts "--slave-host                [#{Configurator.instance.hostname()}]"
    puts "--slave-port                [3306]"
    puts "--slave-user                [#{Configurator.instance.whoami()}]"
    puts "--slave-password"
    puts "--slave-thl-directory       [/opt/continuent/thl]"
    puts "--slave-thl-port            [2112]"
    puts "--slave-relay-directory     [/opt/continuent/relay]"
    puts "--buffer-size               Size of buffers for block commit and queues [#{ReplicationServiceBufferSize.new().get_default_value()}]"
    puts "--channels                  Number of channels for parallel apply [#{ReplicationServiceChannels.new().get_default_value()}]"
    puts "--rmi-port                  [10001]"
    puts "--service-name"
    Configurator.instance.write_divider()
    puts "Install options: --master-slave"
    puts "--dbms-type                 (mysql|postgresql) [mysql]"
    puts "--cluster-hosts"
    puts "--master-host"
    puts "--user"
    puts "--home-directory            [/opt/continuent]"
    puts "--datasource-port           [3306]"
    puts "--datasource-user           [#{Configurator.instance.whoami()}]"
    puts "--datasource-password"
    puts "--datasource-log-directory  [/var/lib/mysql]"
    puts "--datasource-log-pattern    [mysql-bin]"
    puts "--master-log-file           PENDING"
    puts "--master-log-pos            PENDING"
    puts "--datasource-transfer-logs"
    puts "--thl-directory             [/opt/continuent/thl]"
    puts "--thl-port                  [2112]"
    puts "--relay-directory           [/opt/continuent/relay]"
    puts "--buffer-size               Size of buffers for block commit and queues [#{ReplicationServiceBufferSize.new().get_default_value()}]"
    puts "--channels                  Number of channels for parallel apply [#{ReplicationServiceChannels.new().get_default_value()}]"
    puts "--rmi-port                  [10001]"
    puts "--service-name"
    end
end