DBMS_ORACLE = "oracle"

# Oracle-specific parameters.
REPL_ORACLE_SERVICE = "repl_datasource_oracle_service"
REPL_ORACLE_DSPORT = "repl_oracle_dslisten_port"
REPL_ORACLE_HOME = "repl_oracle_home"
REPL_ORACLE_LICENSE = "repl_oracle_license"
REPL_ORACLE_SCHEMA = "repl_oracle_schema"
REPL_ORACLE_LICENSED_SLAVE = "repl_oracle_licensed_slave"

class OracleDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_ORACLE
  end
  
  def get_default_backup_method
    "none"
  end
  
  def get_valid_backup_methods
    "none|script"
  end
  
  def get_thl_uri
	  "jdbc:oracle:thin:@${replicator.global.db.host}:${replicator.global.db.port}:${replicator.applier.oracle.service}"
	end
  
  def get_default_port
    "1521"
  end
  
  def get_default_start_script
    nil
  end
  
  def getJdbcUrl()
    "jdbc:oracle:thin:@${replicator.global.db.host}:${replicator.global.db.port}:${DBNAME}"
  end
  
  def getJdbcDriver()
    "oracle.jdbc.driver.OracleDriver"
  end
  
  def getVendor()
    "oracle"
  end
  
  def get_extractor_template
    raise "Unable to use OracleDatabasePlatform as an extractor"
	end
	
	def get_applier_filters()
	  ["nocreatedbifnotexists","dbupper"] + super()
	end
	
	def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
end

#
# Prompts
#

class OracleConfigurePrompt < ConfigurePrompt
  def get_default_value
    begin
      get_oracle_default_value()
    rescue => e
      @default
    end
  end
  
  def get_oracle_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def oracle(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    raise "Update this to build the proper command"
    ssh_result("echo '#{command}' | psql -q -A -t", true, hostname)
  end
  
  def enabled?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(OracleDatabasePlatform))
  end
end

class OracleService < OracleConfigurePrompt
  include DatasourcePrompt
  
  def initialize
    super(REPL_ORACLE_SERVICE, "Oracle Service", 
      PV_IDENTIFIER)
  end
end

#
# Validation
#

class OracleValidationCheck < ConfigureValidationCheck
  def get_variable(name)
    oracle("show #{name}").chomp.strip;
  end
  
  def enabled?
    super() && @config.getProperty(REPL_DBTYPE) == DBMS_ORACLE
  end
end