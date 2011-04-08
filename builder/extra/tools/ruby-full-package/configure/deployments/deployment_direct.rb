class DirectDeployment < ConfigureDeployment
  def get_name
    "direct"
  end
  
  def get_deployment_configurations()
    config_objs = []
    
    config_obj = Properties.new
    config_obj.props = @config.props.dup
    config_obj.setProperty(DSNAME, config_obj.getProperty(GLOBAL_DSNAME))
    config_objs.push(config_obj)
    
    config_objs
  end
  
  def get_deployment_basedir
    Configurator.instance.get_base_path()
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepDirect
      ]
      
    case @config.getProperty(GLOBAL_DBMS_TYPE)
    when "mysql"
      modules << ConfigureDeploymentStepMySQL
      modules << ConfigureDeploymentStepReplicationDataservice
    when "postgresql"
      modules << ConfigureDeploymentStepPostgresql
    else
      raise "Invalid value for #{GLOBAL_DBMS_TYPE}"
    end
    
    modules << ConfigureDeploymentStepServices
    
    modules
  end
  
  def include_deployment_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    else
      false
    end
  end
  
  def get_default_config_filename
    Configurator.instance().get_base_path() + "/" + Configurator::HOST_CONFIG
  end
end