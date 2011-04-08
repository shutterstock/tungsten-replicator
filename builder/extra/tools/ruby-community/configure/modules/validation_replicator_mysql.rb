class MySQLValidationCheck < ConfigureValidationCheck
  # Execute mysql command and return result to client. 
  def mysql(command, user = nil, password = nil, hostname = nil)
    if user == nil
      user = @config.getProperty(REPL_DBLOGIN)
    end
    if password == nil
      password = @config.getProperty(REPL_DBPASSWORD)
    end
    if hostname == nil
      hostname = @config.getProperty(GLOBAL_HOST)
    end

    cmd_result("mysql -u#{user} -p#{password} -h#{hostname} -e \"#{command}\"", true)
  end
  
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
    (@config.getProperty(GLOBAL_DBMS_TYPE) == "mysql")
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
    @failed_hosts = []
  end
  
  def validate
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      service_hosts = service_properties[REPL_HOSTS].split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        service_hosts.each{
          |repl_host|
          
          login_output = mysql("select 'ALIVE' as 'Return Value'", nil, nil, repl_host)
          if login_output =~ /ALIVE/
            info("MySQL server and login to #{repl_host} is OK")
          else
            help("Run \"GRANT ALL ON *.* TO '#{@config.getProperty(REPL_DBLOGIN)}'@'#{@config.getProperty(GLOBAL_HOST)}' IDENTIFIED BY '#{@config.getProperty(REPL_DBPASSWORD)}' WITH GRANT OPTION\" on #{repl_host}")
            error("MySQL server on #{repl_host} is unavailable or login does not work")
          end
        }
      end
    }
  end
end

class MySQLPermissionsCheck < MySQLValidationCheck
  def set_vars
    @title = "Replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    response = mysql("select * from mysql.user where User = '#{@config.getProperty(REPL_DBLOGIN)}'\\\\G")
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
        error("Missing #{parts[0]} for #{@config.getProperty(REPL_DBLOGIN)}")
      end
    }
    
    if has_missing_priv
      error("The database user is missing some privileges. Run 'mysql -u#{@config.getProperty(REPL_DBLOGIN)} -p#{@config.getProperty(REPL_DBPASSWORD)} -h#{@config.getProperty(GLOBAL_HOST)} -e\"GRANT ALL ON *.* to '#{@config.getProperty(REPL_DBLOGIN)}'@'#{@config.getProperty(GLOBAL_HOST)}' WITH GRANT OPTION\"' on #{@config.getProperty(GLOBAL_HOST)}")
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
    
    info("Check readability of #{@config.getProperty(REPL_MYSQL_BINLOGDIR)}/#{master_file}")
    file_info = cmd_result("file #{@config.getProperty(REPL_MYSQL_BINLOGDIR)}/#{master_file}")
    if file_info =~ /no read permission|cannot open/
      error("Unable to read current binlog file.  Check that this system user can read #{@config.getProperty(REPL_MYSQL_BINLOGDIR)}/#{master_file}.")
    else
      info("The system user is able to read binary logs")
    end
  end
end

class MySQLSettingsCheck < MySQLValidationCheck
  def set_vars
    @title = "MySQL settings check"
    @support_remote_fix = true
  end
  
  def validate
    info("Running SHOW MASTER STATUS to see if replication is enabled")
    show_master = mysql("show master status\\\\G")
    unless show_master =~ /File:/
      error("MySQL binary logs are not configured.")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
    end

    info("Checking server_id")
    server_id = get_value("show variables like 'server_id'", "Value")
    if server_id == nil || server_id == ""
      error("Unable to determine the current server_id")
      help("Add a \"server_id\" value to the MySQL configuration file")
    end
    
    info("Checking sync_binlog setting")
    sync_binlog = get_value("show variables like 'sync_binlog'", "Value")
    if sync_binlog == nil || sync_binlog != "0"
      error("The value of sync_binlog is wrong")
      help("Add \"sync_binlog=0\" to the MySQL configuration file to increase MySQL performance")
    end
    
    info("Checking innodb_flush_log_at_trx_commit")
    innodb_flush_log_at_trx_commit = get_value("show variables like 'innodb_flush_log_at_trx_commit'", "Value")
    if innodb_flush_log_at_trx_commit == nil || innodb_flush_log_at_trx_commit != "2"
      error("The value of innodb_flush_log_at_trx_commit is wrong")
      help("Add \"innodb_flush_log_at_trx_commit=2\" to the MySQL configuration file")
    end
    
    info("Checking max_allowed_packet")
    max_allowed_packet = get_value("show variables like 'max_allowed_packet'", "Value")
    if max_allowed_packet == nil || max_allowed_packet.to_i() < (48*1024*1024)
      error("The value of max_allowed_packet is to small")
      help("Add \"max_allowed_packet=48m\" to the MySQL configuration file")
    end
    
    info("Check for datadir")
    datadir_lines = cmd_result("my_print_defaults mysqld | grep '\\-\\-datadir' | wc -l")
    unless datadir_lines.to_i() > 0
      error("The datadir setting is not specified")
      help("Specify a value for datadir in your my.cnf file to ensure that all utilities work properly")
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
        info("MySQL server and connector login to #{repl_host} is OK")
      else
        help("Run \"GRANT ALL ON *.* TO '#{@config.getProperty(CONN_CLIENTLOGIN)}'@'#{@config.getProperty(GLOBAL_HOST)}' IDENTIFIED BY '#{@config.getProperty(CONN_CLIENTPASSWORD)}'\" on #{repl_host}")
        error("MySQL server on #{repl_host} is unavailable or connector login does not work")
      end
    }
  end
  
  def enabled?
    (super() && is_connector?())
  end
end