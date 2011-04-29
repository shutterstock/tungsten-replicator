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
    @config.props = {}
    @config.setProperty(DEPLOYMENT_TYPE, DIRECT_DEPLOYMENT_NAME)
    
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

    case @config.getProperty(DEPLOYMENT_TYPE)
    when DIRECT_DEPLOYMENT_NAME
      @config.setProperty(DEPLOY_CURRENT_PACKAGE, "true")
      
      options = Properties.new()
      options.props = {
        "master-port" => "3306",
        "master-user" => Configurator.instance.whoami(),
        "slave-port" => "3306",
        "slave-user" => Configurator.instance.whoami(),
        "slave-thl-mode" => "disk",
        "slave-thl-directory" => "/opt/continuent/thl",
        "slave-relay-directory" => "/opt/continuent/relay"
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
        "slave-thl-mode",
        "slave-thl-directory",
        "slave-relay-directory",
        "buffer-size",
        "shard-default-db",
        "allow-bidi-unsafe",
        "allow-any-remote-service",
        "channels",
        "service-name"
      ].each{
        |prop_key|
        opts.on("--#{prop_key} String")  {|val| options.setProperty(prop_key, val)}
      }
      
      begin
        opts.order!(arguments)
      rescue => e
        error("Argument parsing failed: #{e.to_s()}")
        return false
      end
      
      reset_errors()
      
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
        REPL_LOG_DIR => options.getProperty("slave-thl-directory")
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
        REPL_SVC_SHARD_DEFAULT_DB => options.getProperty("shard-default-db"),
        REPL_SVC_ALLOW_BIDI_UNSAFE => options.getProperty("allow-bidi-unsafe"),
        REPL_SVC_ALLOW_ANY_SERVICE => options.getProperty("allow-any-remote-service"),
      })
    when DISTRIBUTED_DEPLOYMENT_NAME
      @config.setProperty(DEPLOY_CURRENT_PACKAGE, "true")
      
      options = Properties.new()
      options.props = {
        "datasource-port" => "3306",
        "datasource-user" => Configurator.instance.whoami(),
        "thl-mode" => "disk",
        "home-directory" => "/opt/continuent",
        "thl-directory" => "/opt/continuent/thl",
        "relay-directory" => "/opt/continuent/relay"
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
        "relay-directory",
        "buffer-size",
        "shard-default-db",
        "allow-bidi-unsafe",
        "allow-any-remote-service",
        "channels",
        "service-name"
      ].each{
        |prop_key|
        opts.on("--#{prop_key} String")  {|val| options.setProperty(prop_key, val)}
      }
      
      begin
        opts.order!(arguments)
      rescue => e
        error("Argument parsing failed: #{e.to_s()}")
        return false
      end
      
      reset_errors()
      
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
      @config.setProperty(REPL_SERVICES, {})
      
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
          REPL_RELAY_LOG_DIR => options.getProperty("relay-directory")
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
          REPL_SVC_SHARD_DEFAULT_DB => options.getProperty("shard-default-db"),
          REPL_SVC_ALLOW_BIDI_UNSAFE => options.getProperty("allow-bidi-unsafe"),
          REPL_SVC_ALLOW_ANY_SERVICE => options.getProperty("allow-any-remote-service"),
        })
        
        if host == options.getProperty("master-host")
          @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "master")
        else
          @config.setProperty([REPL_SERVICES, service_alias, REPL_ROLE], "slave")
          @config.setProperty([REPL_SERVICES, service_alias, REPL_MASTERHOST], options.getProperty("master-host").tr(".", "_"))
        end
      }
    else
      raise "Invalid deployment type specified"
    end
    
    Configurator.instance.save_prompts()
    
    true
  end
  
  def post_prompt_handler_run
    Configurator.instance.save_prompts()
  end
  
  def prepare_saved_config(config)
    temp = config.dup()
    temp.setProperty(REPL_SERVICES, nil)
    temp
  end
end