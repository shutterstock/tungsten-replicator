class MySQLValidationCheck < ConfigureValidationCheck
  include GroupValidationCheckMember
  
  def get_value(command, column)
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
  
  def enabled?
    (@config.getProperty(DBMS_TYPE) == "mysql")
  end
end

class MySQLClientCheck < MySQLValidationCheck
  def set_vars
    @title = "MySQL client check"
  end
  
  def validate
    debug("Checking for an accessible mysql binary")
    mysql = cmd_result("which mysql")
    debug("MySQL client path: #{mysql}")
    
    if mysql == ""
      raise "mysql program not found"
    end
    
    debug("Determine the version of MySQL")
    mysql_version = cmd_result("#{mysql} --version")
    info("MySQL client version: #{mysql_version}")
  end
end

class MySQLLoginCheck < MySQLValidationCheck
  def set_vars
    @title = "Replication credentials login check"
    @fatal_on_error = true
  end
  
  def validate
    login_output = mysql("select 'ALIVE' as 'Return Value'")
    if login_output =~ /ALIVE/
      info("MySQL server and login is OK for #{get_connection_summary()}")
    else
      error("Unable to connect to the MySQL server using #{get_connection_summary()}")
      
      if @config.getProperty(get_member_key(REPL_DBPASSWORD)).to_s() == ""
        help("Try specifying a password for #{get_connection_summary(nil, false)}")
      end
    end
  end
end

class MySQLPermissionsCheck < MySQLValidationCheck
  def set_vars
    @title = "Replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    response = mysql("select * from mysql.user where User = '#{@config.getProperty(get_member_key(REPL_DBLOGIN))}'\\\\G")
    unless $? == 0
      error("Unable to retrieve user permissions")
      help("The #{@config.getProperty(get_member_key(REPL_DBLOGIN))} user must have SUPER,REPLICATION CLIENT permissions")
    end
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == "Host"
        current_host = parts[1]
        command_added = false
      end
      
      if parts[0] =~ /priv/ && parts[1] != "Y"
        has_missing_priv = true
        error("Missing #{parts[0]} for #{@config.getProperty(get_member_key(REPL_DBLOGIN))}")
      end
    }
    
    if has_missing_priv
      error("The database user is missing some privileges. Run 'mysql -u#{@config.getProperty(get_member_key(REPL_DBLOGIN))} -p#{@config.getProperty(get_member_key(REPL_DBPASSWORD))} -h#{@config.getProperty(get_member_key(HOST))} -e\"GRANT ALL ON *.* to '#{@config.getProperty(get_member_key(REPL_DBLOGIN))}'@'#{@config.getProperty(get_member_key(HOST))}' WITH GRANT OPTION\"' on #{@config.getProperty(get_member_key(HOST))}")
    else
      info("All privileges configured correctly")
    end
  end
end

class MySQLReadableLogsCheck < MySQLValidationCheck
  def set_vars
    @title = "Readable binary logs check"
  end
  
  def validate
    master_file = get_value("show master status", "File")
    if master_file == nil
      help("Check that the MySQL user can run \"show master status\"")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
      raise "Unable to determine current binlog file."
    end
    
    info("Check readability of #{@config.getProperty(get_member_key(REPL_MYSQL_BINLOGDIR))}/#{master_file}")
    file_info = cmd_result("file #{@config.getProperty(get_member_key(REPL_MYSQL_BINLOGDIR))}/#{master_file}")
    if file_info =~ /no read permission|cannot open/
      error("Unable to read current binlog file.  Check that this system user can read #{@config.getProperty(get_member_key(REPL_MYSQL_BINLOGDIR))}/#{master_file}.")
    else
      info("The system user is able to read binary logs")
    end
  end
  
  def enabled?
    super() && (@config.getProperty(get_member_key(REPL_DBHOST)) == @config.getProperty(DEPLOYMENT_HOST))
  end
end

class MySQLSettingsCheck < MySQLValidationCheck
  def set_vars
    @title = "MySQL settings check"
    @support_remote_fix = true
  end
  
  def validate
    info("Checking sync_binlog setting")
    sync_binlog = get_value("show variables like 'sync_binlog'", "Value")
    if sync_binlog == nil || sync_binlog != "0"
      warning("The value of sync_binlog is wrong for #{get_connection_summary()}")
      help("Add \"sync_binlog=0\" to the MySQL configuration file to increase MySQL performance for #{get_connection_summary()}")
    end
    
    info("Checking innodb_flush_log_at_trx_commit")
    innodb_flush_log_at_trx_commit = get_value("show variables like 'innodb_flush_log_at_trx_commit'", "Value")
    if innodb_flush_log_at_trx_commit == nil || innodb_flush_log_at_trx_commit != "2"
      warning("The value of innodb_flush_log_at_trx_commit is wrong for #{get_connection_summary()}")
      help("Add \"innodb_flush_log_at_trx_commit=2\" to the MySQL configuration file for #{get_connection_summary()}")
    end
    
    info("Checking max_allowed_packet")
    max_allowed_packet = get_value("show variables like 'max_allowed_packet'", "Value")
    if max_allowed_packet == nil || max_allowed_packet.to_i() < (48*1024*1024)
      warning("The value of max_allowed_packet is too small for #{get_connection_summary()}")
      help("Add \"max_allowed_packet=52m\" to the MySQL configuration file for #{get_connection_summary()}")
    end
    
    if Configurator.instance.is_localhost?(@config.getProperty(HOST))
      info("Check for datadir")
      datadir = get_value("show variables like 'datadir'", "Value")
      unless File.readable?(datadir)
        warning("The datadir setting is not readable for #{get_connection_summary()}")
        help("Specify a readable directory for datadir in your my.cnf file to ensure that all utilities work properly for #{get_connection_summary()}")
      end
    end
  end
end

class ConnectorUserMySQLCheck < MySQLValidationCheck
  def set_vars
    @title = "Connector user check"
  end
  
  def validate
    @config.getProperty(REPL_HOSTS).split(",").each{
      |repl_host|
      login_output = mysql("select 'ALIVE' as 'Return Value'", @config.getProperty(CONN_CLIENTLOGIN), @config.getProperty(CONN_CLIENTPASSWORD), repl_host)
      if login_output =~ /ALIVE/
        info("MySQL server and connector login for #{get_connection_summary()} is OK")
      else
        help("Run \"GRANT ALL ON *.* TO '#{@config.getProperty(CONN_CLIENTLOGIN)}'@'#{@config.getProperty(HOST)}' IDENTIFIED BY '#{@config.getProperty(CONN_CLIENTPASSWORD)}'\" on #{repl_host}")
        error("MySQL server for #{get_connection_summary()} is unavailable or connector login does not work")
      end
    }
  end
  
  def enabled?
    (super() && is_connector?())
  end
end