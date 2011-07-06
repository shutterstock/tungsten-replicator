class MySQLConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
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
  def mysql(command, user = nil, password = nil, hostname = nil, port = nil)
    if user == nil
      user = @config.getProperty(get_member_key(REPL_DBLOGIN))
    end
    
    if password == nil
      password = @config.getProperty(get_member_key(REPL_DBPASSWORD))
    end
    
    if hostname == nil
      hostname = @config.getProperty(get_member_key(REPL_DBHOST))
    end
    
    if port == nil
      port = @config.getPropertyOr(get_member_key(REPL_DBPORT), "3306")
    end

    cmd_result("mysql -u#{user} --password=\"#{password}\" -h#{hostname} --port=#{port} -e \"#{command}\"", true)
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

module MySQLDataservicePrompt
  def enabled?
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    if @config.getProperty([DATASERVERS, applier, DBMS_TYPE]) == "mysql"
      super() && true
    else
      super() && false
    end
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
    
    if master_file_parts.length() > 1
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

class MySQLServerID < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MYSQL_SERVER_ID, "MySQL server ID", 
      PV_INTEGER, 0)
  end
  
  def get_mysql_default_value
    server_id = get_mysql_value("SHOW VARIABLES LIKE 'server_id'", "Value")
    if server_id == nil
      raise "Unable to determine server_id"
    end
    
    server_id
  end
end

class MySQLReplicationUseRelayLogs < MySQLConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_EXTRACTOR_USE_RELAY_LOGS, "Configure the extractor to access the binlog via local relay-logs?",
      PV_BOOLEAN, "true")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
  end
end

class ReplicationServiceUseDrizzle < MySQLAdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_USE_DRIZZLE, "Use the Drizzle MySQL driver", 
      PV_BOOLEAN, "true")
  end
end

class ReplicationServiceSlaveTakeover < ConfigurePrompt
  include NotDeleteServicePrompt
  include GroupConfigurePromptMember
  include MySQLDataservicePrompt
  
  def initialize
    super(REPL_SVC_NATIVE_SLAVE_TAKEOVER, "Takeover native MySQL replication",
      PV_BOOLEAN, "false")
  end
end