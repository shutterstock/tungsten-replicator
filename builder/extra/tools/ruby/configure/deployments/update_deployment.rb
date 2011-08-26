class UpdateDeployment < ConfigureDeployment
  def get_name
    UPDATE_DEPLOYMENT_NAME
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules(config)
    modules = [
      ConfigureDeploymentStepDeployment
      ]
    
    modules << ConfigureDeploymentStepReplicator
    modules << ConfigureDeploymentStepReplicationDataservice
    
    DatabaseTypeDeploymentStep.submodules().each{
      |klass|
      
      modules << klass
    }
    
    modules << ConfigureDeploymentStepServices

    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(UpdatePackage)
      true
    else
      false
    end
  end
end