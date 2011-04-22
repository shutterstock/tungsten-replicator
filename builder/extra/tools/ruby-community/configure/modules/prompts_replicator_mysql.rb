class MySQLConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def get_default_value
    begin
      get_mysql_default_value()
    rescue => e
      @default
    end
  end
  
  def get_mysql_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def mysql(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(GLOBAL_HOSTS).split(",")
      hostname = hosts[0]
    end

    ssh_result("mysql -u#{user} -p#{password} -h#{hostname} --port=#{port} -e '#{command}'", true, hostname)
  end
  
  def get_mysql_value(command, column)
    response = mysql(command + "\\\\G")
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column
        return parts[1]
      end
    }
    
    return nil
  end
end

class MySQLAdvancedPrompt < AdvancedPrompt
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
end

class MySQLBinlogPattern < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_BINLOGPATTERN, "MySQL binlog pattern", PV_ANY, "mysql-bin")
  end
  
  def get_mysql_default_value
    master_file = get_mysql_value("SHOW MASTER STATUS", "File")
    master_file_parts = master_file.split(".")
    
    if master_file_parts.count() > 1
      master_file_parts.pop()
      return master_file_parts.join(".")
    else
      raise "Unable to read the master file"
    end
  end
end

class MySQLBinlogDirectory < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_BINLOGDIR, "MySQL binlog directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
end

class MySQLReplicationServiceMode < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_SVC_MODE, "What type of replication do you want to use for service @value? (#{REPL_MODE_MS}|#{REPL_MODE_DI})",
      PropertyValidator.new("#{REPL_MODE_MS}|#{REPL_MODE_DI}", 
      "Value must be #{REPL_MODE_MS} or #{REPL_MODE_DI}"), "master-slave")
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def get_disabled_value_for_source(parent_name, source_val)
    "master-slave"
  end
end

class MySQLReplicationUseRelayLogs < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_EXTRACTOR_USE_RELAY_LOGS, "Configure the extractor to access the binlog via local relay-logs?",
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty([parent_name, REPL_SVC_MODE]) == "master-slave"
  end
  
  def get_disabled_value_for_source(parent_name, source_val)
    if @config.getProperty([parent_name, REPL_SVC_MODE]) == "direct"
      "true"
    else
      nil
    end
  end
end

class MySQLRelayLogDirectory < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Enter the local-disk directory into which the relay-logs will be stored",
		  PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_HOME_DIRECTORY)
      @config.getProperty(GLOBAL_HOME_DIRECTORY) + "/relay-logs"
    else
      ""
    end
  end
  
  def enabled?
    unless super()
      false
    end
    
    ClusterConfigureModule.each_service(@config) {
      |parent_name, source_val|
      
      if @config.getProperty([parent_name, REPL_EXTRACTOR_USE_RELAY_LOGS]) == "true"
        return true
      end
    }
    
    false
  end
end

class MySQLDirectReplicationExtractHost < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_EXTRACTOR_DBHOST, "Enter the hostname to use when extracting events for service @value",
      PV_IDENTIFIER)
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty([parent_name,REPL_SVC_MODE]) == "direct"
  end
end

class MySQLDirectReplicationExtractPort < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_EXTRACTOR_DBPORT, "Enter the connection port to use when extracting events for service @value",
      PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getProperty(REPL_DBPORT)
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty([parent_name,REPL_SVC_MODE]) == "direct"
  end
end

class MySQLDirectReplicationExtractLogin < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_EXTRACTOR_DBLOGIN, "Enter the username to use when extracting events for service @value",
      PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getProperty(REPL_DBLOGIN)
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty([parent_name,REPL_SVC_MODE]) == "direct"
  end
end

class MySQLDirectReplicationExtractPassword < MultipleValueConfigurePrompt
  def initialize
    super(REPL_SERVICES, Configurator::SERVICE_CONFIG_PREFIX, 
      REPL_EXTRACTOR_DBPASSWORD, "Enter the password to use when extracting events for service @value",
      PV_IDENTIFIER)
  end
  
  def get_default_value
    @config.getProperty(REPL_DBPASSWORD)
  end
  
  def enabled?
    super() && @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
  end
  
  def enabled_for_source?(parent_name, source_val)
    @config.getProperty([parent_name,REPL_SVC_MODE]) == "direct"
  end
end