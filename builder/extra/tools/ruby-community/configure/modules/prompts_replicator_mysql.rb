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
      hosts = @config.getProperty(REPL_HOSTS).split(",")
      hostname = hosts[0]
    end

    cmd_result("mysql -u#{user} -p#{password} -h#{hostname} --port=#{port} -e \"#{command}\"", true)
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
  def initialize
    super(REPL_MYSQL_BINLOGDIR, "MySQL binlog directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
  
  def get_mysql_default_value
    datadir = get_mysql_value("SHOW VARIABLES LIKE 'datadir'", "Value")
    unless datadir == nil
      datadir
    else
      raise "Unable to get value"
    end
  end
end

class THLStorageType < MySQLConfigurePrompt
  def initialize
    super(REPL_LOG_TYPE, "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, "disk")
  end
  
  def enabled?
    super() && Configurator.instance.advanced_mode?()
  end
  
  def get_disabled_value
    "disk"
  end
end

class THLStorageDirectory < MySQLConfigurePrompt
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
  end
  
  def enabled?
    super() && @config.getProperty(REPL_LOG_TYPE) == "disk"
  end
  
  def get_default_value
    if @config.getProperty(GLOBAL_HOME_DIRECTORY)
      @config.getProperty(GLOBAL_HOME_DIRECTORY) + "/thl-logs"
    else
      ""
    end
  end
end

class MySQLRelayLogDirectory < MySQLConfigurePrompt
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
    super() && @config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
  end
end