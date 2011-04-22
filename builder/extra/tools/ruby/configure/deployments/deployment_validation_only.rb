class DeploymentValidationOnly < ConfigureDeployment
  def get_name
    "validation"
  end
  
  def get_deployment_configurations()
    config_objs = []
    hosts = []
    
    @config.getProperty(GLOBAL_HOSTS).each{
      |host_alias, host_props|

      config_obj = Properties.new
      config_obj.props = @config.props.merge(host_props)
    
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def include_deployment_for_package?(package)
    true
  end
end