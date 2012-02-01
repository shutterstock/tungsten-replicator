DBMS_MYSQL = "mysql"

# MySQL-specific parameters
REPL_MYSQL_DATADIR = "repl_datasource_mysql_data_directory"
REPL_MYSQL_RO_SLAVE = "repl_mysql_ro_slave"
REPL_MYSQL_SERVER_ID = "repl_mysql_server_id"
REPL_MYSQL_ENABLE_ENUMTOSTRING = "repl_mysql_enable_enumtostring"
REPL_MYSQL_ENABLE_ANSIQUOTES = "repl_mysql_enable_ansiquotes"
REPL_MYSQL_ENABLE_NOONLYKEYWORDS = "repl_mysql_enable_noonlykeywords"
REPL_MYSQL_XTRABACKUP_DIR = "repl_mysql_xtrabackup_dir"
REPL_MYSQL_XTRABACKUP_FILE = "repl_mysql_xtrabackup_file"
REPL_MYSQL_USE_BYTES_FOR_STRING = "repl_mysql_use_bytes_for_string"
REPL_MYSQL_CONF = "repl_datasource_mysql_conf"

class MySQLDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_MYSQL
  end
  
  def get_default_backup_method
    "mysqldump"
  end
  
  def get_valid_backup_methods
    "none|mysqldump|xtrabackup|script"
  end
  
  # Execute mysql command and return result to client. 
  def run(command)
    begin
      return cmd_result("mysql -u#{@username} --password=\"#{@password}\" -h#{@host} --port=#{@port} -e \"#{command}\"")
    rescue CommandError
      return nil
    end
  end
  
  def get_value(command, column = nil)
    response = run(command + "\\\\G")
    unless response
      return response
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
  
  def get_thl_uri
	  "jdbc:mysql:thin://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.schema}?createDB=true"
	end
  
  def check_thl_schema(thl_schema)
    schemas = run("SHOW SCHEMAS LIKE '#{thl_schema}'")
    if schemas != ""
      raise "THL schema #{thl_schema} already exists at #{get_connection_summary()}"
    end
  end
  
  def get_default_master_log_directory
    "/var/lib/mysql"
  end
  
  def get_default_master_log_pattern
    begin
      master_file = get_value("SHOW MASTER STATUS", "File")
      master_file_parts = master_file.split(".")
    
      if master_file_parts.length() > 1
        master_file_parts.pop()
        return master_file_parts.join(".")
      else
        raise IgnoreError
      end
    rescue
    end
    
    return "mysql-bin"
  end
  
  def get_default_port
    "3306"
  end
  
  def get_default_start_script
    "/etc/init.d/mysql"
  end
  
  def getBasicJdbcUrl()
    "jdbc:mysql:thin://${replicator.global.db.host}:${replicator.global.db.port}/"
  end
  
  def getJdbcUrl()
    "jdbc:mysql:thin://${replicator.global.db.host}:${replicator.global.db.port}/${DBNAME}?jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&allowMultiQueries=true&yearIsDateType=false"
  end
  
  def getJdbcDriver()
    "org.drizzle.jdbc.DrizzleDriver"
  end
  
  def getVendor()
    "mysql"
  end
  
  def getVersion()
    get_value("SHOW VARIABLES LIKE 'version'", "Value")
  end
	
	def get_thl_filters()
	  if @config.getProperty(REPL_MYSQL_ENABLE_ENUMTOSTRING) == "true"
	    ["enumtostring"]
	  else
	    []
	  end
	end

	def get_applier_filters()
   filters =  []
   if @config.getProperty(REPL_MYSQL_ENABLE_ANSIQUOTES) == "true"
      filters += ["ansiquotes"]
   end
   if @config.getProperty(REPL_MYSQL_ENABLE_NOONLYKEYWORDS) == "true"
      filters += ["noonlykeywords"]
   end
	 filters + ["mysqlsessions"] + super()
	end
end

#
# Prompts
#

class MySQLConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_mysql_default_value()
    rescue => e
      super()
    end
  end
  
  def get_mysql_default_value
    raise "Undefined function"
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLDataDirectory < MySQLConfigurePrompt
  include DatasourcePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_DATADIR, "MySQL data directory", 
      PV_FILENAME, "/var/lib/mysql/")
  end
  
  def get_mysql_default_value
    datadir = get_applier_datasource().get_value("SHOW VARIABLES LIKE 'datadir'", "Value")
    if datadir == nil
      raise "Unable to determine datadir"
    end
    
    return datadir
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_mysql_data_dir'))
    super()
  end
end

class MySQLConfFile < ConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_MYSQL_CONF, "MySQL config file", 
      PV_FILENAME, "/etc/my.cnf")
  end
  
  def enabled?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLServerID < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_SERVER_ID, "MySQL server ID", 
      PV_INTEGER)
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      server_id = get_applier_datasource().get_value("SHOW VARIABLES LIKE 'server_id'", "Value")
      if server_id == nil
        raise "Unable to determine server_id"
      end

      server_id
    rescue => e
      super()
    end
  end
  
  def enabled?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
  
  def required?
    false
  end
end

class MySQLEnableEnumToString < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_ENABLE_ENUMTOSTRING, "Expand ENUM values into their text values?", 
      PV_BOOLEAN, "false")
  end
  
  def get_default_value
    if get_extractor_datasource().class != get_applier_datasource().class
      return "true"
    end
    
    super()
  end
end

class MySQLEnableAnsiQuotes < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_ENABLE_ANSIQUOTES, "Enables ANSI_QUOTES mode for incoming events?", 
      PV_BOOLEAN, "false")
  end
end

class MySQLEnableNoOnlyKeywords < ConfigurePrompt
  include ReplicationServicePrompt
  
  def initialize
    super(REPL_MYSQL_ENABLE_NOONLYKEYWORDS, "Translates DELETE FROM ONLY -> DELETE FROM and UPDATE ONLY -> UPDATE.", 
      PV_BOOLEAN, "false")
  end
end

class MySQLUseBytesForStrings < ConfigurePrompt
  include ReplicationServicePrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_MYSQL_USE_BYTES_FOR_STRING, "Transfer strings as their byte representation?", 
      PV_BOOLEAN, "true")
  end
  
  def get_default_value
    if get_extractor_datasource().class != get_applier_datasource().class
      return "false"
    end
    
    super()
  end
end

class MySQLXtrabackupDirectory < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_DIR, "Directory to use for xtrabackup temp files", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_BACKUP_DUMP_DIR)) + "/innobackup"
  end
end

class MySQLXtrabackupFile < ConfigurePrompt
  include ReplicationServicePrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_MYSQL_XTRABACKUP_FILE, "File to use for xtrabackup packaging", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_BACKUP_DUMP_DIR)) + "/innobackup.tar"
  end
end

#
# Validation
#

module MySQLApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(MySQLDatabasePlatform))
  end
end

module MySQLExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(MySQLDatabasePlatform))
  end
end

class MySQLClientCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

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

class MySQLLoginCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "Replication credentials login check"
    @fatal_on_error = true
  end
  
  def validate
    login_output = get_applier_datasource.run("select 'ALIVE' as 'Return Value'")
    if login_output =~ /ALIVE/
      info("MySQL server and login is OK for #{get_applier_datasource.get_connection_summary()}")
    else
      error("Unable to connect to the MySQL server using #{get_applier_datasource.get_connection_summary()}")
      
      if get_applier_datasource().password.to_s() == ""
        help("Try specifying a password for #{get_applier_datasource.get_connection_summary()}")
      end
    end
  end
end

class MySQLPermissionsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  include NotPrefetchCheck

  def set_vars
    @title = "Replication user permissions check"
  end
  
  def validate
    has_missing_priv = false
    
    user = get_applier_datasource.get_value("select user()", "user()")
    grants = get_applier_datasource.get_value("show grants")
    
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

class MySQLBinaryLogsEnabledCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLExtractorCheck

  def set_vars
    @title = "Binary logs enabled check"
  end
  
  def validate
    log_bin = get_extractor_datasource.get_value("show variables like 'log_bin'", "Value")
    if log_bin != "ON"
      help("Check that the MySQL user can run \"show variables like 'log_bin'\"")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
      error("Binary logs are not enabled on #{get_extractor_datasource.get_connection_summary}")
    end
  end
end

class MySQLReadableLogsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLExtractorCheck

  def set_vars
    @title = "Readable binary logs check"
  end
  
  def validate
    master_file = get_extractor_datasource.get_value("show master status", "File")
    if master_file == nil
      help("Check that the MySQL user can run \"show master status\"")
      help("Add \"log-bin=mysql-bin\" to the MySQL configuration file.")
      raise "Unable to determine current binlog file."
    end
    
    info("Check readability of #{@config.getProperty(get_member_key(REPL_LOGDIR))}/#{master_file}")
    file_info = cmd_result("file #{@config.getProperty(get_member_key(REPL_LOGDIR))}/#{master_file}")
    if file_info =~ /no read permission|cannot open/
      error("Unable to read current binlog file.  Check that this system user can read #{@config.getProperty(get_member_key(REPL_LOGDIR))}/#{master_file}.")
    else
      info("The system user is able to read binary logs")
    end
  end
  
  def enabled?
    super() && 
      (get_extractor_datasource().host == 
        @config.getProperty([HOSTS, @config.getProperty(DEPLOYMENT_HOST), HOST])) && 
      (@config.getProperty(get_member_key(REPL_DISABLE_RELAY_LOGS)) == "true")
  end
end

class MySQLConfigFileCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  include NotPrefetchCheck
  
  def set_vars
    @title = "MySQL config file is available"
  end
  
  def validate
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    
    if Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      unless File.exists?(conf_file)
        error("The MySQL config file '#{conf_file}' does not exist")
      else
        unless File.readable?(conf_file)
          error("The MySQL config file '#{conf_file}' is not readable")
        end
      end
    else
      warning("Unable to check the MySQL config file '#{conf_file}'")
    end
  end
end

class MySQLExtractorServerIDCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLExtractorCheck
  include NotPrefetchCheck
  
  def set_vars
    @title = "MySQL Direct Replication Server ID"
  end
  
  def validate
    conf_file = @config.getProperty(get_extractor_key(REPL_MYSQL_CONF))
    if Configurator.instance.is_localhost?(@config.getProperty(get_extractor_key(REPL_DBHOST)))
      if File.exists?(conf_file) && File.readable?(conf_file)
        begin
          conf_file_results = cmd_result("grep ^server-id #{conf_file}").split("=")
        rescue
          error("The MySQL config file '#{conf_file}' does not include a value for server-id")
          help("Check the file to ensure a value is given and that it is not commented out")
        end
      else
        error("The MySQL config file '#{conf_file}' is not readable")
      end
    else
      warning("Unable to check for a configured server-id in '#{conf_file}' on #{get_extractor_datasource.get_connection_summary}")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
  end
end

class MySQLApplierServerIDCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  include NotPrefetchCheck
  
  def set_vars
    @title = "MySQL Server ID"
  end
  
  def validate
    server_id = @config.getProperty(get_member_key(REPL_MYSQL_SERVER_ID))
    if server_id.to_i <= 0
      error("The server-id '#{server_id}' for #{get_applier_datasource.get_connection_summary()} is too small")
    elsif server_id.to_i > 4294967296
      error("The server-id '#{server_id}' for #{get_applier_datasource.get_connection_summary()} is too large")
    end
    
    retrieved_server_id = get_applier_datasource.get_value("SHOW VARIABLES LIKE 'server_id'", "Value")
    if server_id.to_i != retrieved_server_id.to_i
      error("The server-id '#{server_id}' does not match the the server-id from #{get_applier_datasource.get_connection_summary()} '#{retrieved_server_id}'")
    end
    
    conf_file = @config.getProperty(get_applier_key(REPL_MYSQL_CONF))
    if Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      if File.exists?(conf_file) && File.readable?(conf_file)
        begin
          conf_file_results = cmd_result("grep ^server-id #{conf_file}").split("=")
          if conf_file_results.length <= 1
            error("Unable to read the server-id value in the MySQL config file '#{conf_file_results.join('=')}'")
          else
            unless server_id.to_i == conf_file_results[1].to_i
              error("The MySQL config file has a server-id of '#{conf_file_results[1].to_i}' that is different from the configured server-id")
            end
          end
        rescue
          error("The MySQL config file '#{conf_file}' does not include a value for server-id")
          help("Check the file to ensure a value is given and that it is not commented out")
        end
      else
        error("The MySQL config file '#{conf_file}' is not readable")
      end
    else
      warning("Unable to compare the configured server-id to the one in '#{conf_file}'")
    end
  end
end

class MySQLSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck

  def set_vars
    @title = "MySQL settings check"
  end
  
  def validate
    info("Checking sync_binlog setting")
    sync_binlog = get_applier_datasource.get_value("show variables like 'sync_binlog'", "Value")
    if sync_binlog == nil || sync_binlog != "0"
      warning("The value of sync_binlog is wrong for #{get_applier_datasource.get_connection_summary()}")
      help("Add \"sync_binlog=0\" to the MySQL configuration file to increase MySQL performance for #{get_applier_datasource.get_connection_summary()}")
    end
    
    info("Checking innodb_flush_log_at_trx_commit")
    innodb_flush_log_at_trx_commit = get_applier_datasource.get_value("show variables like 'innodb_flush_log_at_trx_commit'", "Value")
    if innodb_flush_log_at_trx_commit == nil || innodb_flush_log_at_trx_commit != "2"
      warning("The value of innodb_flush_log_at_trx_commit is wrong for #{get_applier_datasource.get_connection_summary()}")
      help("Add \"innodb_flush_log_at_trx_commit=2\" to the MySQL configuration file for #{get_applier_datasource.get_connection_summary()}")
    end
    
    info("Checking max_allowed_packet")
    max_allowed_packet = get_applier_datasource.get_value("show variables like 'max_allowed_packet'", "Value")
    if max_allowed_packet == nil || max_allowed_packet.to_i() < (48*1024*1024)
      warning("The value of max_allowed_packet is too small for #{get_applier_datasource.get_connection_summary()}")
      help("Add \"max_allowed_packet=52m\" to the MySQL configuration file for #{get_applier_datasource.get_connection_summary()}")
    end
  end
end

class MySQLNoMySQLReplicationCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  include NotPrefetchCheck
  
  def set_vars
    @title = "No MySQL replication check"
  end
  
  def validate
    info("Checking that MySQL replication is not running on the slave datasource")
    slave_sql_running = get_applier_datasource.get_value("SHOW SLAVE STATUS", "Slave_SQL_Running")
    if (slave_sql_running != nil) and (slave_sql_running != "No")
      error("The slave datasource #{get_applier_datasource.get_connection_summary()} has a running slave SQL thread")
    end
  end
  
  def enabled?
    if @config.getProperty(get_member_key(REPL_SVC_NATIVE_SLAVE_TAKEOVER)) == "false"
      super()
    else
      false
    end
  end
end

class MysqldumpAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Mysqldump method availability check"
  end
  
  def validate
    path = cmd_result("which mysqldump")
    info("mysqldump found at #{path}")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "mysqldump"
  end
end

class XtrabackupAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Xtrabackup availability check"
  end
  
  def validate
    begin
      path = cmd_result("which innobackupex-1.5.1")
      info("xtrabackup found at #{path}")
    rescue
      error("Unable to find the innobackupex-1.5.1 script for backup")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "xtrabackup"
  end
end

class XtrabackupSettingsCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include MySQLApplierCheck
  
  def set_vars
    @title = "Xtrabackup settings"
  end
  
  def validate
    unless Configurator.instance.is_localhost?(@config.getProperty(get_applier_key(REPL_DBHOST)))
      error("Xtrabackup may only be used to backup a database server running on the same host as Tungsten Replicator")
      return
    end
    
    unless File.executable?(@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT)))
      if File.exists?(@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT)))
        error("The MySQL service script #{@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT))} is not executable")
      else
        error("The MySQL service script #{@config.getProperty(get_applier_key(REPL_BOOT_SCRIPT))} does not exist")
      end
      help("Try providing a value for ---datasource-boot-script")
    end
    
    if @config.getProperty(get_member_key(REPL_BACKUP_COMMAND_PREFIX)) != "true"
      error("You must enable sudo  to use xtrabackup")
      help("Add --backup-command-prefix=true to your command")
    end
    
    info("Check for datadir")
    datadir = get_applier_datasource.get_value("show variables like 'datadir'", "Value")
    unless File.directory?(datadir)
      warning("The datadir setting #{datadir} is not readable for #{get_applier_datasource.get_connection_summary()}")
      help("Specify a readable directory for datadir in your my.cnf file to ensure that all utilities work properly for #{get_applier_datasource.get_connection_summary()}")
    end
    unless @config.getProperty(get_applier_key(REPL_MYSQL_DATADIR)) == datadir
      error("The MySQL datadir setting does not match the provided value: #{@config.getProperty(get_applier_key(REPL_MYSQL_DATADIR))}")
      help("Fix the datadir value in my.cnf or use '--datasource-mysql-data-directory=#{datadir}'")
    end
    
    info("Check for binary logs")
    binary_count = cmd_result("#{@config.getTemplateValue(get_member_key(REPL_BACKUP_COMMAND_PREFIX))} ls #{@config.getProperty(get_applier_key(REPL_MASTER_LOGDIR))}/#{@config.getProperty(get_applier_key(REPL_MASTER_LOGPATTERN))}.* 2>/dev/null | wc -l")
    unless binary_count.to_i > 0
      error("#{@config.getProperty(get_applier_key(REPL_MASTER_LOGDIR))} does not contain any files starting with #{@config.getProperty(get_applier_key(REPL_MASTER_LOGPATTERN))}")
      help("Try providing a value for --datasource-log-directory or --datasource-log-pattern")
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "xtrabackup"
  end
end

module ConfigureDeploymentStepMySQL
  include DatabaseTypeDeploymentStep
  
  def get_backup_config
    if @config.getProperty(REPL_BACKUP_METHOD) == "xtrabackup"
      mkdir_if_absent(@config.getProperty(REPL_MYSQL_XTRABACKUP_DIR))
    end
    
    super()
  end
end
