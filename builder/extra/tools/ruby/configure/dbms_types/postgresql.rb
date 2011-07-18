DBMSTypePrompt.add_dbms_type(DBMS_POSTGRESQL)

#
# Prompts
#

class PostgresConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(get_member_key(DBMS_TYPE)) == "postgresql"
  end
  
  def get_default_value
    begin
      get_pgsql_default_value()
    rescue => e
      @default
    end
  end
  
  def get_pgsql_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def pgsql(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    ssh_result("echo '#{command}' | psql -q -A -t", true, hostname)
  end
end

class PostgresStreamingReplication < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_STREAMING, "Use streaming replication (available from PostgreSQL 9)",
      PV_BOOLEAN, "false")
  end
end

class PostgresRootDirectory < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_ROOT, "Root directory for postgresql installation", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(HOME_DIRECTORY)
    super()
  end
  
  def get_pgsql_default_value
    def_data_dir = pgsql("SHOW data_directory;")
    if def_data_dir.to_s() == "" || !def_data_dir.include?("/") || def_data_dir.include?("psql")
      raise "Cannot determine"
    else
      def_data_dir[0, def_data_dir.rindex('/')]
    end
  end
end

class PostgresDataDirectory < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_HOME, "PostgreSQL data directory", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(REPL_PG_ROOT) + "/data"
  end
  
  def get_default_value
    @default = @config.getProperty(HOME_DIRECTORY)
    super()
  end
  
  def get_pgsql_default_value
    def_data_dir = pgsql("SHOW data_directory;")
    if def_data_dir.to_s() == ""
      raise "Cannot determine"
    else
      def_data_dir
    end
  end
end

class PostgresArchiveDirectory < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_ARCHIVE, "PostgreSQL archive location", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_PG_ROOT)) + "/archive"
  end
end

class PostgresConfFile < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_POSTGRESQL_CONF, "Location of postgresql.conf", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(get_member_key(REPL_PG_ROOT)) + "/data/postgresql.conf"
  end
  
  def get_pgsql_default_value
    def_config_path = pgsql("SHOW config_file;")
    if def_config_path.to_s() == ""
      raise "Cannot determine"
    else
      def_config_path
    end
  end
end

class PostgresArchiveTimeout < PostgresConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_PG_ARCHIVE_TIMEOUT, "Timeout for sending unfilled WAL buffers (data loss window)", 
      PV_INTEGER, 60)
  end
end

#
# Validation
#

class PostgreSQLValidationCheck < ConfigureValidationCheck
  def get_variable(name)
    psql("show #{name}").chomp.strip;
  end
  
  def enabled?
    super() && @config.getProperty(DBMS_TYPE) == "postgresql"
  end
end

class PostgreSQLSystemUserCheck < PostgreSQLValidationCheck
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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
  include DataserverValidationCheck

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

#
# Deployment
#

module ConfigureDeploymentStepPostgresql
  include DatabaseTypeDeploymentStep
  
  def apply_config_replicator
    write_wal_shipping_properties()
  end
  
  def deploy_replication_dataservice(service_name, service_config)
    if is_applier(service_config, DBMS_POSTGRESQL) && 
        is_extractor(service_config, DBMS_POSTGRESQL)
      write_wal_shipping_properties(service_name, service_config)
    end
    
    super(service_name, service_config)
  end
  
  def transform_replication_dataservice_line(line, service_name, service_config)
    if line =~ /replicator.master.uri/ && 
        is_applier(service_config, DBMS_POSTGRESQL) then
      if service_config.getProperty(REPL_MASTERHOST)
        "replicator.master.uri=wal://" + service_config.getProperty(REPL_MASTERHOST) + "/"
      else
        "replicator.master.uri=wal://localhost/"
      end
    elsif line =~ /replicator.master.connect.uri/ && 
        is_applier(service_config, DBMS_POSTGRESQL) then
      if service_config.getProperty(REPL_ROLE) == "master" then
        line
      else
        "replicator.master.connect.uri=thl://" + service_config.getProperty(REPL_MASTERHOST) + "/"
      end
    elsif line =~ /replicator.script.root/ && 
        is_applier(service_config, DBMS_POSTGRESQL)
      "replicator.script.root_dir=" + File.expand_path("#{get_deployment_basedir()}/tungsten-replicator")
    elsif line =~ /replicator.script.conf_file/ && 
        is_applier(service_config, DBMS_POSTGRESQL)
      "replicator.script.conf_file=conf/postgresql-wal.properties"
    elsif line =~ /replicator.script.processor/ && 
        is_applier(service_config, DBMS_POSTGRESQL)
      "replicator.script.processor=bin/pg/pg-wal-plugin"
    elsif line =~ /replicator.backup.agent.pg_dump.port/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.port=" + service_config.getProperty(REPL_DBPORT)
    elsif line =~ /replicator.backup.agent.pg_dump.user/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.user=" + service_config.getProperty(REPL_DBLOGIN)
    elsif line =~ /replicator.backup.agent.pg_dump.password/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.password=" + service_config.getProperty(REPL_DBPASSWORD)
    elsif line =~ /replicator.backup.agent.pg_dump.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
		else
		  super(line, service_name, service_config)
		end
	end
  
  def write_wal_shipping_properties(service_name, service_config)
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.postgresql-wal.properties",
        "#{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties", "# ")
    
    transformer.transform { |line|
      if line =~ /postgresql.streaming_replication/ then
        "postgresql.streaming_replication=" + service_config.getProperty(REPL_PG_STREAMING)
      elsif line =~ /postgresql.data/ then
        "postgresql.data=" + service_config.getProperty(REPL_PG_HOME)
      elsif line =~ /postgresql.conf/ then
        "postgresql.conf=" + service_config.getProperty(REPL_PG_POSTGRESQL_CONF)
      elsif line =~ /^\s*postgresql.pg_standby=\s*(\S*)\s*$/ then
        # For pg_standby to work we need the full path of whatever is in 
        # the sample file. 
        pg_standby = which $1
        # If that does not work, try looking for the binaries under roo. 
        if ! pg_standby
          pg_standby = which(service_config.getProperty(REPL_PG_ROOT) + "/bin/pg_standby")
        end
        if pg_standby
          "postgresql.pg_standby=" + pg_standby
        else
           raise RemoteError, "Unable to locate pg_standby; please ensure it is defined correctly in tungsten-replicator/conf/postgresql-wal.properties" 
        end
      elsif line =~ /^\s*postgresql.pg_archivecleanup=\s*(\S*)\s*$/ then
        pg_archivecleanup = which $1
        if ! pg_archivecleanup
          pg_archivecleanup = which(service_config.getProperty(REPL_PG_ROOT) + "/bin/pg_archivecleanup")
        end
        if pg_archivecleanup
          "postgresql.pg_archivecleanup=" + pg_archivecleanup
        elsif @config.getProperty(REPL_PG_STREAMING) == "true"
           raise RemoteError, "Unable to locate pg_archivecleanup; please ensure it is defined correctly in tungsten-replicator/conf/postgresql-wal.properties" 
        end
      elsif line =~ /postgresql.archive_timeout/ then
        "postgresql.archive_timeout=" + service_config.getProperty(REPL_PG_ARCHIVE_TIMEOUT)
      elsif line =~ /postgresql.archive/ then
        "postgresql.archive=" + service_config.getProperty(REPL_PG_ARCHIVE)
      elsif line =~ /postgresql.role/ then
        "postgresql.role=" + service_config.getProperty(REPL_ROLE)
      elsif line =~ /postgresql.master.host/ && service_config.getProperty(REPL_ROLE) == REPL_ROLE_M then
        "postgresql.master.host=" + service_config.getProperty(REPL_MASTERHOST)
      elsif line =~ /postgresql.master.user/ then
        "postgresql.master.user=" + service_config.getProperty(REPL_DBLOGIN)
      elsif line =~ /postgresql.master.password/ then
        "postgresql.master.password=" + service_config.getProperty(REPL_DBPASSWORD)
      elsif line =~ /postgresql.boot.script/ then
        "postgresql.boot.script=" + service_config.getProperty(REPL_BOOT_SCRIPT)
      elsif line =~ /postgresql.root.prefix/ then
        "postgresql.root.prefix=" + get_root_prefix()
      else
        line
      end
    }
  end
  
  # Perform installation of required replication support files.  
  def postgresql_configuration
    # Select installation command. 
    info("Running procedure to configure warm standby...")
    cmd1 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg/pg-wal-plugin -o uninstall -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    cmd2 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg/pg-wal-plugin -o install -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    info("############ RESTART=#{@config.getProperty(SVC_START)} ##################")
    
    if (@config.getProperty(SVC_START) == "false")
      cmd2 = cmd2 + " -s"
    end
    
    Configurator.instance.write_divider()
    # Do uninstall first. 
    cmd_result(cmd1, true)
    # Then the install, which has to success. 
    cmd_result(cmd2, false)
    Configurator.instance.write_divider()
  end
  
  def get_replication_dataservice_template(service_config)
    if is_applier(service_config, DBMS_POSTGRESQL) && 
        is_extractor(service_config, DBMS_POSTGRESQL)
  	  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.static.properties.postgresql"
  	else
  	  super(service_config)
  	end
	end
end