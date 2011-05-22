class DirectDeployment < ConfigureDeployment
  def get_name
    DIRECT_DEPLOYMENT_NAME
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules(config)
    modules = [
      ConfigureDeploymentStepDirect
      ]
    
    modules << ConfigureDeploymentStepReplicator  
    modules << ConfigureDeploymentStepReplicationDataservice
    modules << ConfigureDeploymentStepMySQL
    modules << ConfigureDeploymentStepServices
    
    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      false
    elsif package.is_a?(ReplicatorInstallPackage)
      false
    else
      false
    end
  end
  
  def get_default_config_filename
    Configurator.instance().get_base_path() + "/" + Configurator::HOST_CONFIG
  end
  
  def require_deployment_host
    true
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses
  end
end