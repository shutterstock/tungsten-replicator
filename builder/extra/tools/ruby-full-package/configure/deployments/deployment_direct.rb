DIRECT_DEPLOYMENT_NAME = "regular"
DIRECT_DEPLOYMENT_HOST_ALIAS = "local"
class DirectDeployment < ConfigureDeployment
  def get_name
    DIRECT_DEPLOYMENT_NAME
  end
  
  def get_deployment_configurations()
    config_objs = [@config.dup()]
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepDirect
      ]
      
    modules << ConfigureDeploymentStepReplicationDataservice
    case @config.getProperty(DBMS_TYPE)
    when "mysql"
      modules << ConfigureDeploymentStepMySQL
    when "postgresql"
      modules << ConfigureDeploymentStepPostgresql
    else
      raise "Invalid value for #{DBMS_TYPE}"
    end
    
    modules << ConfigureDeploymentStepServices
    
    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    elsif package.is_a?(ReplicatorInstallPackage)
      true
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
end