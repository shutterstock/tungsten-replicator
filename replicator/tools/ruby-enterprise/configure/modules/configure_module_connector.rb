class ConnectorConfigureModule < ConfigureModule
  def initialize
    super()
    @weight = 20
  end
  
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      ConnectorHostsPrompt.new(),
      ConfigurePrompt.new(CONN_LISTEN_PORT, "Listen port for connector", 
        PV_INTEGER, 9999),
      ConfigurePrompt.new(CONN_RWSPLITTING, "Enable read/write splitting", 
        PV_BOOLEAN, "false"),
      ConfigurePrompt.new(CONN_DELETE_USER_MAP, "Regenerate user.map if file already exists", 
        PV_BOOLEAN, "false"),
      ConnectorUserLoginPrompt.new(),
      ConnectorUserPasswordPrompt.new(),
      ConfigurePrompt.new(CONN_CLIENTDEFAULTDB, "Application client default database", 
        PV_IDENTIFIER, "test"),
      AdvancedPrompt.new(SQLR_USENEWPROTOCOL, "Use the new SQL Router protocol",
        PV_BOOLEAN, "true"),
      AdvancedPrompt.new(ROUTER_WAITFOR_DISCONNECT, "How many times should the router attempt to reconnect",
        PV_INTEGER, 5),
      AdvancedPrompt.new(SQLR_DELAY_BEFORE_OFFLINE, "Number of seconds before an isolated router should go OFFLINE", PV_INTEGER, 600),
      ConstantValuePrompt.new(SQLR_KEEP_ALIVE_TIMEOUT, "Timeout for SQL Router keep-alive calls", PV_INTEGER, 0),
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
      ConnectorListenPortCheck.new(),
    ])
  end
end

class ConnectorHostsPrompt < ConfigurePrompt
  def initialize
    super(CONN_HOSTS, "Enter a comma-delimited list of connector hosts", 
      PV_HOSTNAME)
  end
  
  def get_default_value
    @config.getProperty(GLOBAL_HOSTS)
  end
end

class ConnectorUserLoginPrompt < ConfigurePrompt
  def initialize
    super(CONN_CLIENTLOGIN, "Application client login", PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getProperty(REPL_DBLOGIN)
  end
end

class ConnectorUserPasswordPrompt < ConfigurePrompt
  def initialize
    super(CONN_CLIENTPASSWORD, "Application client password", PV_ANY)
  end
  
  def get_default_value
    @config.getProperty(REPL_DBPASSWORD)
  end
end

class ConnectorListenPortCheck < ConfigureValidationCheck
  def set_vars
    @title = "Connector port check"
  end
  
  def validate
    non_java_listening_processes = cmd_result("sudo netstat -nap | grep \":#{@config.getProperty(CONN_LISTEN_PORT)}\\b\" | grep -v java | wc -l")
    
    if non_java_listening_processes.to_i() > 0
      error("There is a non-java process listenening on port #{@config.getProperty(CONN_LISTEN_PORT)}")
      help("Specify a different port for the connector or re-configure the process listening on that port")
    else
      info("The connector port is available")
    end
  end
end