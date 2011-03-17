module ConfigureDeploymentStepConnector
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_connector")
    ]
  end
  module_function :get_deployment_methods
  
  # Configure the Tungsten Connector
  def deploy_connector
    unless is_connector?()
      info("Tungsten Connector is not active; skipping configuration")
      return
    end

    Configurator.instance.write_header("Performing Tungsten Connector configuration...")

    svc = @config.getProperty(GLOBAL_DSNAME)
    user = @config.getProperty(CONN_CLIENTLOGIN)
    password = @config.getProperty(CONN_CLIENTPASSWORD)
    db = @config.getProperty(CONN_CLIENTDEFAULTDB)

    # Fix up the connector.properties.
    connector_properties_in = "#{get_deployment_basedir()}/tungsten-connector/conf/sample.connector.properties"
    # Use existing connector.properties if it's already in place
    if File.exists?("#{get_deployment_basedir()}/tungsten-connector/conf/connector.properties")
      connector_properties_in = "#{get_deployment_basedir()}/tungsten-connector/conf/connector.properties";
    end
    transformer = Transformer.new(
      connector_properties_in,
      "#{get_deployment_basedir()}/tungsten-connector/conf/connector.properties", nil)

    transformer.transform { |line|
      if line =~ /manageTransactionsLocally/
        # Must be turned on for SQL Router session consistency to work.
        "manageTransactionsLocally = true"
      elsif line =~ /forcedDBforUnspecConnections/ then
        # Automatically connect to specified database
        "forcedDBforUnspecConnections=" + db
      elsif line =~ /^server.port/
        "server.port=" + @config.getProperty(CONN_LISTEN_PORT)
      elsif @config.getProperty(GLOBAL_DBMS_TYPE) == "postgresql" and line =~ /server.version = /
        "server.version = 8.4.1"
      elsif @config.getProperty(GLOBAL_DBMS_TYPE) == "postgresql" and line =~ /server.protocol =/
        "server.protocol = postgresql"
      else
        line
      end
    }

    # Fix up the user.map file.  Note that if user does not wish to delete
    # existing file we don't do that.
    user_map = "#{get_deployment_basedir()}/tungsten-connector/conf/user.map"
    if File.exists?(user_map) && @config.getProperty(CONN_DELETE_USER_MAP) == "false"
      info("NOTE: File user.map already exists and delete option is false")
      info("File not regenerated: #{user_map}")
    else
      transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-connector/conf/sample.user.map", 
        user_map, "# ")

      transformer.transform { |line|
        if line =~ /realuser realpass default/ then
          if password == "" || password == nil then
            user + " - " + svc
          else
            user + " " + password + " " + svc
          end
        elsif line =~ /^@direct/ then
          if @config.getProperty(CONN_RWSPLITTING) == "true" then
            "@direct " + user + " jdbc:t-router://" + svc + "/${DBNAME}?qos=RO_RELAXED&user=" + user + "&password=" + password
          else
            "#" + line
          end
        else
          line
        end
      }
      
      unless File.chmod(0600, user_map) == 1
        error("Unable to set the file permissions for #{user_map}")
      end
    end

    # Configure user name in service script.
    set_run_as_user("#{get_deployment_basedir()}/tungsten-connector/bin/connector")

    # When using the old protocol the connector could not be started until
    # the manager had time sto startup.
    # TENT-90: Register connector.properties but *do not* add it as a service.
    # TUC-22: this is not required anymore with new protocol
    if @config.getProperty(SQLR_USENEWPROTOCOL) == "true" then
      add_service("tungsten-connector/bin/connector")
    end
    
    FileUtils.cp("#{get_deployment_basedir()}/tungsten-connector/conf/connector.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(GLOBAL_DSNAME)}/service/connector.properties")
  end
  
  def is_connector?
    (@config.getProperty(CONN_HOSTS).split(",").include?(@config.getProperty(GLOBAL_HOST)))
  end
end