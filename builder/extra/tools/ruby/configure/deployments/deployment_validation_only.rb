class DeploymentValidationOnly < ConfigureDeployment
  def get_name
    "validation"
  end
  
  def get_deployment_configurations()
    config_objs = []
    
    @config.getProperty(GLOBAL_HOSTS).split(",").each{
      |deployment_host|
      config_obj = Properties.new
      config_obj.props = @config.props.dup
      config_obj.setProperty(DSNAME, config_obj.getProperty(GLOBAL_DSNAME))
      config_obj.setProperty(GLOBAL_HOST, deployment_host)
      config_obj.setProperty(GLOBAL_IP_ADDRESS, Resolv.getaddress(deployment_host))
      
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def include_deployment_for_package?(package)
    true
  end
end