raise IgnoreError
DBMS_POSTGRESQL = "postgresql"

# PostgreSQL-specific parameters.
REPL_PG_METHOD = "repl_pg_method"
REPL_PG_HOME = "repl_datasource_pg_home"
REPL_PG_ROOT = "repl_datasource_pg_root"
REPL_PG_CONF = "repl_datasource_pg_conf"
REPL_PG_ARCHIVE = "repl_datasource_pg_archive"
REPL_PG_ARCHIVE_TIMEOUT = "repl_pg_archive_timeout"
REPL_PG_STREAMING = "repl_pg_streaming"
REPL_PG_WAL_ROLE = "repl_pg_wal_role"
REPL_PG_STANDBY = "repl_pg_standby"
REPL_PG_STANDBYTRIGGER = "repl_pg_standbytrigger"
REPL_PG_ARCHIVECLEANUP = "repl_pg_archivecleanup"

PG_REPL_METHOD_STREAMING = "streaming"
PG_REPL_METHOD_SHIPPING = "shipping"
PG_REPL_METHOD_SLONY = "slony"

class PGDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_POSTGRESQL
  end
  
  def get_default_backup_method
    "pg_dump"
  end
  
  def get_valid_backup_methods
    "none|pg_dump|script"
  end
  
  def run(command)
    begin
      ssh_result("echo '#{command}' | psql -q -A -t", @host, @username)
    rescue RemoteError
      return ""
    end
  end
  
  def get_variable(name)
    run("show #{name}").chomp.strip;
  end
  
  def get_thl_uri
	  "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/"
	end
  
  def get_default_port
    "5432"
  end
  
  def get_default_start_script
    "/etc/init.d/postgres"
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def getJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcDriver()
    "org.postgresql.Driver"
  end
  
  def getVendor()
    "postgresql"
  end
	
	def get_applier_filters
	  ["pgddl"]
	end
end

#
# Prompts
#

module PGDatasourcePrompt
  def enabled?
    super() && (get_datasource().is_a?(PGDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(PGDatabasePlatform))
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_pgsql_default_value()
    rescue => e
      super()
    end
  end
  
  def get_pgsql_default_value
    raise "Undefined function"
  end
end

class PostgresStreamingReplication < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_PG_METHOD, "Postgres Replication method",
      PV_BOOLEAN)
  end
  
  def get_default_value
    if (get_applier_datasource().is_a?(PGDatabasePlatform) && get_extractor_datasource().is_a?(PGDatabasePlatform))
      PG_REPL_METHOD_STREAMING
    else
      PG_REPL_METHOD_SLONY
    end
  end
  
  def enabled?
    super() && get_extractor_datasource().is_a?(PGDatabasePlatform)
  end
  
  def enabled_for_config?
    super() && get_extractor_datasource().is_a?(PGDatabasePlatform)
  end
end

class PostgresRootDirectory < ConfigurePrompt
  include DatasourcePrompt
  include PGDatasourcePrompt
  
  def initialize
    super(REPL_PG_ROOT, "Root directory for postgresql installation", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty([HOSTS, @config.getProperty(get_member_key(HOST)), HOME_DIRECTORY])
    super()
  end
  
  def get_pgsql_default_value
    def_data_dir = get_datasource.run("SHOW data_directory;")
    if def_data_dir.to_s() == "" || !def_data_dir.include?("/") || def_data_dir.include?("psql")
      raise "Cannot determine"
    else
      def_data_dir[0, def_data_dir.rindex('/')]
    end
  end
end

class PostgresDataDirectory < ConfigurePrompt
  include DatasourcePrompt
  include PGDatasourcePrompt
  
  def initialize
    super(REPL_PG_HOME, "PostgreSQL data directory", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_PG_ROOT), true) + "/data"
  end
end

class PostgresArchiveDirectory < ConfigurePrompt
  include DatasourcePrompt
  include PGDatasourcePrompt
  
  def initialize
    super(REPL_PG_ARCHIVE, "PostgreSQL archive location", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_PG_ROOT), true) + "/archive"
  end
end

class PostgresConfFile < ConfigurePrompt
  include DatasourcePrompt
  include PGDatasourcePrompt
  
  def initialize
    super(REPL_PG_CONF, "Location of postgresql.conf", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(get_member_key(REPL_PG_HOME), true) + "/postgresql.conf"
    super()
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

class PostgresArchiveTimeout < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_PG_ARCHIVE_TIMEOUT, "Timeout for sending unfilled WAL buffers (data loss window)", 
      PV_INTEGER, 60)
  end
end

class PostgresUseStreaming < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PG_STREAMING, "Use PG streaming replication (true|false)", PV_BOOLEAN)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_PG_METHOD)) == PG_REPL_METHOD_STREAMING
      "true"
    else
      "false"
    end
  end
end

class PostgresWALRole < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PG_WAL_ROLE, "Role for PG WAL shipping", PV_IDENTIFIER)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_M
      "master"
    else
      "slave"
    end
  end
end

class PostgresStandbyPath < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PG_STANDBY, "Path to the pg_standby script", PV_FILENAME)
  end
  
  def get_default_value
    pg_standby = cmd_result("which pg_standby", true)
    
    # If that does not work, try looking for the binaries under roo. 
    if pg_standby.to_s == ""
      pg_standby = @config.getProperty(get_applier_key(REPL_PG_ROOT), true) + "/bin/pg_standby"
    end

    return pg_standby
  end
end

class PostgresStandbyTriggerPath < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PG_STANDBYTRIGGER, "Path to the trigger file for pg_standby", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_host_key(TEMP_DIRECTORY)) + "/pgsql.trigger"
  end
end

class PostgresArchiveCleanupPath < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_PG_ARCHIVECLEANUP, "Path to the pg_archivecleanup script", PV_FILENAME)
  end
  
  def get_default_value
    pg_archivecleanup = cmd_result("which pg_archivecleanup", true)
    
    # If that does not work, try looking for the binaries under roo. 
    if pg_archivecleanup.to_s == ""
      pg_archivecleanup = @config.getProperty(get_applier_key(REPL_PG_ROOT), true) + "/bin/pg_archivecleanup"
    end
    
    return pg_archivecleanup
  end
end

#
# Validation
#

module PGApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(PGDatabasePlatform))
  end
end

module PGExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(PGDatabasePlatform))
  end
end

class PostgreSQLSystemUserCheck < ConfigureValidationCheck
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

class PostgreSQLClientCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include PGApplierCheck
  
  def set_vars
    @title = "PostgreSQL datasource client check"
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

class PostgreSQLLoginCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include PGApplierCheck

  def set_vars
    @title = "Replication credentials login check"
  end
  
  def validate
    login_output = get_applier_datasource().run("select 'ALIVE' as \\\"Return Value\\\"")
    if login_output =~ /ALIVE/
      info("PostgreSQL server and login to #{get_applier_datasource().get_connection_summary()} is OK")
    else
      error("PostgreSQL server on #{get_applier_datasource().get_connection_summary()} is unavailable or login does not work")
    end
  end
end

class PostgreSQLStandbyCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include PGApplierCheck

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

class PostgreSQLPermissionsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include PGApplierCheck

  def set_vars
    @title = "PostgreSQL file locations check"
  end
  
  def validate
    # Home
    unless File.readable?(@config.getProperty(get_applier_key(REPL_PG_ROOT)))
      error("Unable to read the Postgres root directory at #{@config.getProperty(get_applier_key(REPL_PG_ROOT))}")
    else
      info("#{@config.getProperty(get_applier_key(REPL_PG_ROOT))} is readable")
    end
    
    # Data
    unless File.readable?(@config.getProperty(get_applier_key(REPL_PG_HOME)))
      error("Unable to read the Postgres data directory at #{@config.getProperty(get_applier_key(REPL_PG_HOME))}")
    else
      info("#{@config.getProperty(get_applier_key(REPL_PG_HOME))} is readable")
    end
    
    # Archive
    unless File.writable?(@config.getProperty(get_applier_key(REPL_PG_ARCHIVE)))
      error("Unable to read the Postgres archive directory at #{@config.getProperty(get_applier_key(REPL_PG_ARCHIVE))}")
    else
      info("#{@config.getProperty(get_applier_key(REPL_PG_ARCHIVE))} is writable")
    end
    
    # postgresql.conf
    unless File.writable?(@config.getProperty(get_applier_key(REPL_PG_CONF)))
      error("Unable to write the postgresql.conf file at #{@config.getProperty(get_applier_key(REPL_PG_CONF))}")
    else
      info("#{@config.getProperty(get_applier_key(REPL_PG_CONF))} is writable")
    end
    
    # init script
    unless File.executable?(@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT)))
      error("Unable to execute the Postgres init script at #{@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT))}")
    else
      info("#{@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT))} is executable")
    end
  end
end

class PostgreSQLSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include PGApplierCheck

  def set_vars
    @title = "PostgreSQL settings check"
  end
  
  def validate
    listen_addresses = get_applier_datasource().get_variable("listen_addresses")
    if listen_addresses =~ /\*/
      info("PostgreSQL server is listen on all addresses")
    else
      error("PostgreSQL server is only listening on #{listen_addresses}.  Add \"listen_addresses = '*'\" to postgresql.conf and restart the service.")
    end
  end
end

class PgdumpAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Pg_dump method availability check"
  end
  
  def validate
    path = cmd_result("which pg_dump")
    info("pg_dump found at #{path}")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "pg_dump"
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
  
  def deploy_replication_dataservice()
    if [PG_REPL_METHOD_STREAMING, PG_REPL_METHOD_SHIPPING].include?(@config.getProperty(REPL_PG_METHOD))
      write_wal_shipping_properties()
    end
    
    super()
  end
  
  def write_wal_shipping_properties()
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/postgresql-wal.properties.tpl",
        "#{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties", "# ")
    
    transformer.transform_values(method(:transform_replication_dataservice_values))
    transformer.output
    
    postgresql_configuration
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
  
  def get_replication_dataservice_template()
    if [PG_REPL_METHOD_STREAMING, PG_REPL_METHOD_SHIPPING].include?(@config.getProperty(REPL_PG_METHOD))
      "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.postgresql.tpl"
    else
      super()
    end
	end
end