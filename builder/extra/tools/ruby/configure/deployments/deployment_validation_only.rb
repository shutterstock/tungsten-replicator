class DeploymentValidationOnly < ConfigureDeployment
  def get_name
    "validation"
  end
  
  def get_deployment_configurations()
    config_objs = []
    
    @config.getProperty(CLUSTER_DEPLOYMENTS).each{
      |deployment_alias, deployment_config|
      config_obj = Properties.new
      config_obj.props = @config.props.dup
      
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def include_deployment_for_package?(package)
    case package.class().name()
    when ConfigurePackageDeleteService
      false
    else
      true
    end
  end
end