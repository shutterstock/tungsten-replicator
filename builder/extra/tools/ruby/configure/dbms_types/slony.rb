DBMS_SLONY = "slony"

class SlonyDatabasePlatform < ConfigureDatabasePlatform
  def get_uri_scheme
    DBMS_SLONY
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
    "jdbc:postgresql://${replicator.global.db.host}:${replicator.global.db.port}/${DBNAME}"
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

module SlonyDatasourcePrompt
  def enabled?
    super() && (get_datasource().is_a?(SlonyDatabasePlatform))
  end
  
  def enabled_for_config?
    super() && (get_datasource().is_a?(SlonyDatabasePlatform))
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      get_slony_default_value()
    rescue => e
      super()
    end
  end
  
  def get_slony_default_value
    raise "Undefined function"
  end
end

#
# Validation
#

module SlonyApplierCheck
  def enabled?
    super() && (get_applier_datasource().is_a?(SlonyDatabasePlatform))
  end
end

module SlonyExtractorCheck
  def enabled?
    super() && (get_extractor_datasource().is_a?(SlonyDatabasePlatform))
  end
end

#
# Deployment
#

module ConfigureDeploymentStepSlony
  include DatabaseTypeDeploymentStep
  
  def deploy_replication_dataservice()
    if (get_extractor_datasource().is_a?(SlonyDatabasePlatform) || get_applier_datasource().is_a?(SlonyDatabasePlatform))
      deploy_slony()
    end
    
    super()
  end
  
  def deploy_slony()
  end
end