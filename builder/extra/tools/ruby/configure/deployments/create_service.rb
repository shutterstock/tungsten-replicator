class CreateServiceDeployment < ConfigureDeployment
  def get_name
    ConfigureServicePackage::SERVICE_CREATE
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules(config)
    modules = [
      ConfigureDeploymentStepReplicationDataservice,
      DeploymentStepCreateService,
      ConfigureDeploymentStepMySQL
    ]

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