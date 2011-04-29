class DeleteServiceDeployment < ConfigureDeployment
  def get_name
    ConfigureServicePackage::SERVICE_DELETE
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepReplicationDataservice,
      DeploymentStepDeleteService
    ]
      
    case @config.getProperty(DBMS_TYPE)
    when "mysql"
      modules << ConfigureDeploymentStepMySQL
    when "postgresql"
      modules << ConfigureDeploymentStepPostgresql
    else
      raise "Invalid value for #{DBMS_TYPE}"
    end

    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(ConfigureServicePackage)
      true
    else
      false
    end
  end
  
  def require_deployment_host
    true
  end
  
  def expand_deployment_configuration(deployment_config)
    config = super(deployment_config)
    
    applier_dataserver = config.getProperty(REPL_DATASERVER)
    config.props = config.props.merge(config.getPropertyOr([DATASERVERS, applier_dataserver], {}))

    config.setProperty(REPL_LOG_DIR, config.getProperty(REPL_LOG_DIR) + "/" + config.getProperty(DEPLOYMENT_SERVICE))
    config.setProperty(REPL_RELAY_LOG_DIR, config.getProperty(REPL_RELAY_LOG_DIR) + "/" + config.getProperty(DEPLOYMENT_SERVICE))
    config.setDefault(REPL_SVC_CONFIG_FILE, 
      "#{config.getProperty(BASEDIR)}/tungsten-replicator/conf/static-#{config.getProperty(DEPLOYMENT_SERVICE)}.properties")
    
    config
  end
end