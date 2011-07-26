DBMSTypePrompt.add_dbms_type(DBMS_MYSQL)

#
# Prompts
#

class MySQLConfigurePrompt < ConfigurePrompt
  def enabled?
    super() && @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
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

class MySQLAdvancedPrompt < MySQLConfigurePrompt
  include AdvancedPromptModule
end

class MySQLDataDirectory < MySQLConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_MYSQL_DATADIR, "MySQL data directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
end

class MySQLBinlogDirectory < MySQLConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_MYSQL_BINLOGDIR, "MySQL binlog directory", 
      PV_FILENAME)
  end
  
  def get_default_value
    @config.getPropertyOr(get_member_key(REPL_MYSQL_DATADIR), "/var/lib/mysql")
  end
end

class MySQLBinlogPattern < MySQLConfigurePrompt
  include DataserverPrompt
  
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

class MySQLServerID < MySQLConfigurePrompt
  include DataserverPrompt
  
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

class MySQLReplicationUseRelayLogs < MySQLAdvancedPrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_EXTRACTOR_USE_RELAY_LOGS, "Configure the extractor to access the binlog via local relay-logs?",
      PV_BOOLEAN, "true")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
  end
end

class ReplicationServiceUseDrizzle < MySQLAdvancedPrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_USE_DRIZZLE, "Use the Drizzle MySQL driver", 
      PV_BOOLEAN, "true")
  end
end

class MySQLEnableEnumToString < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_ENABLE_ENUMTOSTRING, "Expand ENUM values into their text values?", 
      PV_BOOLEAN)
  end
  
  def get_default_value
    extractor = @config.getProperty(get_member_key(REPL_EXTRACTOR_DATASERVER))
    
    if extractor
      applier = @config.getProperty(get_member_key(REPL_DATASERVER))
      if @config.getProperty([DATASERVERS, extractor, DBMS_TYPE]) != 
          dbms_type = @config.getProperty([DATASERVERS, applier, DBMS_TYPE])
        return "true"
      end
    end
    
    return "false"
  end
  
  def enabled?
    extractor = @config.getProperty(get_member_key(REPL_EXTRACTOR_DATASERVER))
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    
    if extractor
      dbms_type = @config.getProperty([DATASERVERS, extractor, DBMS_TYPE])
    else
      dbms_type = @config.getProperty([DATASERVERS, applier, DBMS_TYPE])
    end
    
    super() && (dbms_type == DBMS_MYSQL)
  end
end

#
# Validation
#

class MySQLValidationCheck < ConfigureValidationCheck
  def get_value(command, column = nil, on_datasource = nil)
    if on_datasource == nil
      response = mysql(command + "\\\\G")
    else
      response = mysql_on(command + "\\\\G", on_datasource)
    end
    
    response.split("\n").each{ | response_line |
      parts = response_line.chomp.split(":")
      if (parts.length != 2)
        next
      end
      parts[0] = parts[0].strip;
      parts[1] = parts[1].strip;
      
      if parts[0] == column || column == nil
        return parts[1]
      end
    }
    
    return nil
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(DBMS_TYPE)) == "mysql"
  end
end

class MySQLClientCheck < MySQLValidationCheck
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

  def set_vars
    @title = "Replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    user = get_value("select user()", "user()")
    grants = get_value("show grants")
    
    info("Checking user permissions: #{grants}")
    unless grants =~ /ALL PRIVILEGES/
      has_missing_priv = true
    end
    
    unless grants =~ /WITH GRANT OPTION/
      has_missing_priv = true
    end
    
    if has_missing_priv
      error("The database user is missing some privileges or the grant option. Run 'mysql -u#{@config.getProperty(get_member_key(REPL_DBLOGIN))} -p#{@config.getProperty(get_member_key(REPL_DBPASSWORD))} -h#{@config.getProperty(get_member_key(REPL_DBHOST))} -e\"GRANT ALL ON *.* to #{user} WITH GRANT OPTION\"'")
    else
      info("All privileges configured correctly")
    end
  end
end

class MySQLReadableLogsCheck < MySQLValidationCheck
  include DataserverValidationCheck

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
    super() && 
      (@config.getProperty(get_member_key(REPL_DBHOST)) == 
        @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])) && 
      (@config.getProperty(get_member_key(REPL_EXTRACTOR_USE_RELAY_LOGS)) == "false")
  end
end

class MySQLApplierServerIDCheck < MySQLValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "MySQL Server ID"
  end
  
  def validate
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    server_id = @config.getProperty([DATASERVERS, applier, REPL_MYSQL_SERVER_ID])
    if server_id.to_i <= 0
      error("The server-id '#{server_id}' for #{get_connection_summary_for(applier)} is too small")
    elsif server_id.to_i > 4294967296
      error("The server-id '#{server_id}' for #{get_connection_summary_for(applier)} is too large")
    end
    
    retrieved_server_id = get_value("SHOW VARIABLES LIKE 'server_id'", "Value", applier)
    if server_id.to_i != retrieved_server_id.to_i
      error("The server-id '#{server_id}' does not match the the server-id from #{get_connection_summary_for(applier)} '#{retrieved_server_id}'")
    end
  end
  
  def enabled?
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    if @config.getProperty([DATASERVERS, applier, DBMS_TYPE]) == "mysql"
      true
    else
      false
    end
  end
end

class MySQLSettingsCheck < MySQLValidationCheck
  include DataserverValidationCheck

  def set_vars
    @title = "MySQL settings check"
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
  include DataserverValidationCheck

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

class MySQLNoMySQLReplicationCheck < MySQLValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "No MySQL replication check"
  end
  
  def validate
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    info("Checking that MySQL replication is not running on the slave datasource")
    slave_sql_running = get_value("SHOW SLAVE STATUS", "Slave_SQL_Running", applier)
    if (slave_sql_running != nil) and (slave_sql_running != "No")
      error("The slave datasource #{get_connection_summary_for(applier)} has a running slave SQL thread")
    end
  end
  
  def enabled?
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    if @config.getProperty([DATASERVERS, applier, DBMS_TYPE]) == "mysql" &&
        @config.getProperty(get_member_key(REPL_SVC_NATIVE_SLAVE_TAKEOVER)) == "false"
      true
    else
      false
    end
  end
end

#
# Deployment
#

module ConfigureDeploymentStepMySQL
  include DatabaseTypeDeploymentStep
  
  def transform_replication_dataservice_line(line, service_name, service_config)
		if line =~ /replicator.store.thl.url/ &&
		    is_applier(service_config, DBMS_MYSQL) then
			if (service_config.getProperty(REPL_USE_DRIZZLE) == "true")
				"replicator.store.thl.url=jdbc:mysql:thin://" +
				  service_config.getProperty(REPL_DBHOST) + ":" +
				  service_config.getProperty(REPL_DBPORT) +
				  "/tungsten_${service.name}?createDB=true"
			else
				"replicator.store.thl.url=jdbc:mysql://" +
				  service_config.getProperty(REPL_DBHOST) + ":" +
				  service_config.getProperty(REPL_DBPORT) +
				  "/tungsten_${service.name}?createDatabaseIfNotExist=true"
			end
		elsif line =~ /replicator.extractor.mysql.binlog_dir/ &&
  		  is_extractor(service_config, DBMS_MYSQL) && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) != "true" then
			"replicator.extractor.mysql.binlog_dir=" + 
			  service_config.getProperty(REPL_MYSQL_BINLOGDIR)
		elsif line =~ /replicator.extractor.mysql.binlog_file_pattern/ &&
    		is_extractor(service_config, DBMS_MYSQL) then
			"replicator.extractor.mysql.binlog_file_pattern=" + 
			  service_config.getProperty(REPL_MYSQL_BINLOGPATTERN)
		elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/ && 
		    service_config.getProperty(REPL_BACKUP_METHOD) == "mysqldump"
			"replicator.backup.agent.mysqldump.dumpDir=" + 
			  service_config.getProperty(REPL_BACKUP_DUMP_DIR)
		elsif line =~ /replicator.extractor.mysql.usingBytesForString/ &&
		    is_extractor(service_config, DBMS_MYSQL)
			"replicator.extractor.mysql.usingBytesForString=" + 
			  service_config.getProperty(REPL_USE_DRIZZLE)
		elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && 
		    is_extractor(service_config, DBMS_MYSQL) && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.useRelayLogs=" + 
			  service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS)
		elsif line =~ /replicator.extractor.mysql.relayLogDir/ &&
		    is_extractor(service_config, DBMS_MYSQL) && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.relayLogDir=" + 
			  service_config.getProperty(REPL_RELAY_LOG_DIR)
		elsif line =~ /replicator.extractor.mysql.serverId/ &&
		    is_extractor(service_config, DBMS_MYSQL) && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
		  "replicator.extractor.mysql.serverId=#{service_config.getProperty(REPL_MYSQL_SERVER_ID)}"
		elsif line =~ /replicator.stage.q-to-thl.filters=/ &&
		    service_config.getProperty(REPL_MYSQL_ENABLE_ENUMTOSTRING) == "true"
		  "replicator.stage.q-to-thl.filters=enumtostring"
		elsif line =~ /replicator.stage.d-q-to-thl.filters=/ &&
  		  service_config.getProperty(REPL_MYSQL_ENABLE_ENUMTOSTRING) == "true"
		  "replicator.stage.d-q-to-thl.filters=enumtostring"
		elsif line =~ /^replicator.backup.agent.xtrabackup.options/ && 
		    service_config.getPropertyOr(REPL_BACKUP_METHOD) == "xtrabackup"
      directory = service_config.getProperty(REPL_BACKUP_DUMP_DIR) + "/innobackup"
      unless mkdir_if_absent(directory)
        raise "Unable to create directory #{directory} for storing temporary Xtrabackup files"
      end
      
      archive = service_config.getProperty(REPL_BACKUP_DUMP_DIR) + "/innobackup.tar"
            
      "replicator.backup.agent.xtrabackup.options=" +
      "user=${replicator.global.db.user}&" +
      "password=${replicator.global.db.password}" + 
      "&host=#{service_config.getProperty(REPL_DBHOST)}" +
      "&port=#{service_config.getProperty(REPL_DBPORT)}" + 
      "&directory=#{directory}&archive=#{archive}" +
      "&mysqldatadir=#{service_config.getProperty(REPL_MYSQL_DATADIR)}" +
      "&mysql_service_command=#{service_config.getProperty(REPL_BOOT_SCRIPT)}"
    elsif line =~ /^replicator.nativeSlaveTakeover/ &&
        is_applier(service_config, DBMS_MYSQL)
      "replicator.nativeSlaveTakeover=#{service_config.getPropertyOr(REPL_SVC_NATIVE_SLAVE_TAKEOVER, 'false')}"
		else
		  super(line, service_name, service_config)
		end
	end
	
	def get_replication_dataservice_template(service_config)
    if is_applier(service_config, DBMS_MYSQL) && 
        is_extractor(service_config, DBMS_MYSQL)
      if service_config.getProperty(REPL_USE_DRIZZLE) == "true"
  			"#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql-with-drizzle-driver"
  		else
  		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql"
  		end
  	else
  	  super(service_config)
  	end
	end
end