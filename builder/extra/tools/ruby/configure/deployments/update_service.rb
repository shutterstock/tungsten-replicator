class UpdateServiceDeployment < ConfigureDeployment
  def get_name
    ConfigureServicePackage::SERVICE_UPDATE
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepReplicationDataservice,
      DeploymentStepUpdateService
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
end