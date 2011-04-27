class CreateServiceDeployment < ConfigureDeployment
  def get_name
    ConfigureServicePackage::SERVICE_CREATE
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepReplicationDataservice,
      DeploymentStepCreateService
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
    
    config.props = config.props.merge(config.getPropertyOr([DATASERVERS, config.getProperty(REPL_DATASERVER)], {}))
    
    case config.getProperty(REPL_ROLE)
    when REPL_ROLE_M
    when REPL_ROLE_S
    when REPL_ROLE_DI
      config.setProperty(REPL_EXTRACTOR_USE_RELAY_LOGS, "true")
      
      direct_datasource = config.getProperty(REPL_EXTRACTOR_DATASERVER)
      config.setDefault(REPL_EXTRACTOR_DBHOST, config.getProperty([DATASERVERS, direct_datasource, REPL_DBHOST]))
      config.setDefault(REPL_EXTRACTOR_DBPORT, config.getProperty([DATASERVERS, direct_datasource, REPL_DBPORT]))
      config.setDefault(REPL_EXTRACTOR_DBLOGIN, config.getProperty([DATASERVERS, direct_datasource, REPL_DBLOGIN]))
      config.setDefault(REPL_EXTRACTOR_DBPASSWORD, config.getProperty([DATASERVERS, direct_datasource, REPL_DBPASSWORD]))
    end

    config.setProperty(REPL_LOG_DIR, config.getProperty(REPL_LOG_DIR) + "/" + config.getProperty(DEPLOYMENT_SERVICE))
    config.setProperty(REPL_RELAY_LOG_DIR, config.getProperty(REPL_RELAY_LOG_DIR) + "/" + config.getProperty(DEPLOYMENT_SERVICE))
    config.setDefault(REPL_SVC_CONFIG_FILE, 
      "#{config.getProperty(BASEDIR)}/tungsten-replicator/conf/static-#{config.getProperty(DEPLOYMENT_SERVICE)}.properties")

    config
  end
end