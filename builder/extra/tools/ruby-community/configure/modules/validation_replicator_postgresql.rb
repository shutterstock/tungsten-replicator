class PostgreSQLValidationCheck < ConfigureValidationCheck
  include GroupValidationCheckMember
  
  # Execute mysql command and return result to client. 
  def psql(command, user = nil, password = nil, hostname = nil, port = nil)
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
      port = @config.getPropertyOr(get_member_key(REPL_DBPORT), "5432")
    end

    cmd_result("psql -U#{user} -h#{hostname} --port=#{port} -t -c \"#{command}\"")
  end
  
  def get_variable(name)
    psql("show #{name}").chomp.strip;
  end
  
  def enabled?
    (@config.getProperty(DBMS_TYPE) == "postgresql")
  end
end

class PostgreSQLSystemUserCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "PostgreSQL system user check"
  end
  
  def validate
    login = (ENV['USERNAME'] or ENV['USER'])
    if login.to_s() != "postgres" and login.to_s() != "enterprisedb" then
      error("You must extract and configure Tungsten with 
        the database system user.  This is usually 'postgres' for PostgreSQL 
        and 'enterprisedb' for EnterpriseDB.  Your 
        current user is: #{login}.  We recommend you do not use this user.")
    end
    
    if @config.getProperty(USERID) != "postgres" and 
        @config.getProperty(USERID) != "enterprisedb" then
      error("You must run Tungsten with 
        the database system user.  This is usually 'postgres' for PostgreSQL 
        and 'enterprisedb' for EnterpriseDB.  You have specified 
        #{@config.getProperty(USERID)}.  We recommend you do not use this user.")
    end
  end
end

class PostgreSQLClientCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "PostgreSQL client check"
  end
  
  def validate
    debug("Checking for an accessible psql binary")
    psql = cmd_result("which psql")
    debug("PostgreSQL client path: #{psql}")
    
    if psql == ""
      raise "psql program not found"
    end
    
    debug("Determine the version of PostgreSQL")
    psql_version = cmd_result("#{psql} -V")
    info("PostgreSQL client version: #{psql_version}")
  end
end

class PostgreSQLLoginCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "Replication credentials login check"
  end
  
  def validate
    login_output = psql("select 'ALIVE' as \\\"Return Value\\\"", nil, nil, repl_host)
    if login_output =~ /ALIVE/
      info("PostgreSQL server and login to #{@config.getProperty(get_member_key(REPL_DBHOST))} is OK")
    else
      error("PostgreSQL server on #{@config.getProperty(get_member_key(REPL_DBHOST))} is unavailable or login does not work")
    end
  end
end

class PostgreSQLStandbyCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "pg_standby availability check"
  end
  
  def validate
    pg_standby = cmd_result("which pg_standby")
    info("pg_standby client path: #{pg_standby}")
    if pg_standby == ""
      raise "pg_standby program not found in execution path"
    end

    # Execute to ensure it works. 
    pg_standby_version = cmd_result("#{pg_standby} --version")
    info("pg_standby version: #{pg_standby_version}")
  end
end

class PostgreSQLPermissionsCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "PostgreSQL file locations check"
  end
  
  def validate
    # Home
    unless File.readable?(@config.getProperty(get_member_key(REPL_PG_ROOT)))
      error("Unable to read the Postgres root directory at #{@config.getProperty(get_member_key(REPL_PG_ROOT))}")
    else
      info("#{@config.getProperty(get_member_key(REPL_PG_ROOT))} is readable")
    end
    
    # Data
    unless File.readable?(@config.getProperty(get_member_key(REPL_PG_HOME)))
      error("Unable to read the Postgres data directory at #{@config.getProperty(get_member_key(REPL_PG_HOME))}")
    else
      info("#{@config.getProperty(get_member_key(REPL_PG_HOME))} is readable")
    end
    
    # Archive
    unless File.writable?(@config.getProperty(get_member_key(REPL_PG_ARCHIVE)))
      error("Unable to read the Postgres archive directory at #{@config.getProperty(get_member_key(REPL_PG_ARCHIVE))}")
    else
      info("#{@config.getProperty(get_member_key(REPL_PG_ARCHIVE))} is writable")
    end
    
    # postgresql.conf
    unless File.writable?(@config.getProperty(get_member_key(REPL_PG_POSTGRESQL_CONF)))
      error("Unable to write the postgresql.conf file at #{@config.getProperty(get_member_key(REPL_PG_POSTGRESQL_CONF))}")
    else
      info("#{@config.getProperty(get_member_key(REPL_PG_POSTGRESQL_CONF))} is writable")
    end
    
    # init script
    unless File.executable?(@config.getProperty(get_member_key(REPL_BOOT_SCRIPT)))
      error("Unable to execute the Postgres init script at #{@config.getProperty(get_member_key(REPL_BOOT_SCRIPT))}")
    else
      info("#{@config.getProperty(get_member_key(REPL_BOOT_SCRIPT))} is executable")
    end
  end
end

class PostgreSQLSettingsCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "PostgreSQL settings check"
  end
  
  def validate
    listen_addresses = get_variable("listen_addresses")
    if listen_addresses =~ /\*/
      info("PostgreSQL server is listen on all addresses")
    else
      error("PostgreSQL server is only listening on #{listen_addresses}.  Add \"listen_addresses = '*'\" to postgresql.conf and restart the service.")
    end
  end
end

class ConnectorUserPostgreSQLCheck < PostgreSQLValidationCheck
  def set_vars
    @title = "Connector user check"
  end
  
  def validate
    login_output = psql("select 'ALIVE' as \\\"Return Value\\\"", @config.getProperty(CONN_CLIENTLOGIN), @config.getProperty(CONN_CLIENTPASSWORD), repl_host)
    if login_output =~ /ALIVE/
      info("PostgreSQL server and connector login to #{@config.getProperty(get_member_key(REPL_DBHOST))} is OK")
    else
      error("PostgreSQL server on #{get_member_key(REPL_DBHOST)} is unavailable or connector login does not work")
    end
  end
  
  def enabled?
    (super() && is_connector?())
  end
end