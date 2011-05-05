class MySQLConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(DBMS_TYPE) == "mysql"
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
      hosts = @config.getProperty(HOSTS).split(",")
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
    super() && @config.getProperty(DBMS_TYPE) == "mysql"
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

class MySQLDataDirectory < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_DATADIR, "MySQL data directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
end

class MySQLBinlogDirectory < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_BINLOGDIR, "MySQL binlog directory", 
      PV_FILENAME)
  end
  
  def get_default_value
    @config.getPropertyOr(get_member_key(REPL_MYSQL_DATADIR), "/var/lib/mysql")
  end
end

class MySQLReplicationUseRelayLogs < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_EXTRACTOR_USE_RELAY_LOGS, "Configure the extractor to access the binlog via local relay-logs?",
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(DBMS_TYPE) == "mysql"
  end
end

class MySQLRelayLogDirectory < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Enter the local-disk directory into which the relay-logs will be stored",
		  PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY))
      @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/relay"
    else
      ""
    end
  end
end

class ReplicationServiceUseDrizzle < MySQLAdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_USE_DRIZZLE, "Use the Drizzle MySQL driver", 
      PV_BOOLEAN, "true")
  end
end