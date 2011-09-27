DBMS_POSTGRESQL = "postgresql"

class PostgreSQLDatabasePlatform < ConfigureDatabasePlatform
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
      ssh_result("echo '#{command}' | psql -q -A -t -p #{port}", @host, @username)
    rescue RemoteError
      return ""
    end
  end
  
  def get_variable(name)
    run("show #{name}").chomp.strip;
  end
  
  def get_thl_uri
	  "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.extractor.dbms.database}"
	end
  
  def get_default_port
    "5432"
  end
  
  def get_default_start_script
    "/etc/init.d/postgres"
  end
  
  def get_thl_filters()
    ["dropcomments"] + super()
  end
  
  def get_default_master_log_directory
    nil
  end
  
  def get_default_master_log_pattern
    nil
  end
  
  def getJdbcUrl()
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/${replicator.extractor.dbms.database}"
  end
  
  def getJdbcDriver()
    "org.postgresql.Driver"
  end
  
  def getVendor()
    "postgresql"
  end
end

#
# Prompts
#

module PostgreSQLDatasourcePrompt
  def enabled?
    super() && (get_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_postgresql_default_value()
    rescue => e
      super()
    end
  end
  
  def get_postgresql_default_value
    raise "Undefined function"
  end
end

REPL_POSTGRESQL_DBNAME = "repl_postgresql_dbname"
class PostgreSQLDatabaseName < ConfigurePrompt
 include ReplicationServicePrompt
 
 def initialize
   super(REPL_POSTGRESQL_DBNAME, "Name of the database to replicate",
     PV_ANY)
 end
   
 def enabled?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform)
 end
 
 def enabled_for_config?
   super() && get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform)
 end
end

#
# Validation
#

module PostgreSQLApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
end

module SlonyExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform))
  end
end

#
# Deployment
#

module ConfigureDeploymentStepPostgreSQL
  include DatabaseTypeDeploymentStep
  
  def deploy_replication_dataservice()
    if (get_extractor_datasource().is_a?(PostgreSQLDatabasePlatform) || get_applier_datasource().is_a?(PostgreSQLDatabasePlatform))
      deploy_postgresql()
    end
    
    super()
  end
  
  def deploy_postgresql()
  end
end