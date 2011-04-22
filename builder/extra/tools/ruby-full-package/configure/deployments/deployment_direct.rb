class DirectDeployment < ConfigureDeployment
  def get_name
    "regular"
  end
  
  def get_deployment_configurations()
    config_objs = []
    
    @config.getProperty(HOSTS).each{
      |host_alias, host_props|
      
      unless host_props[HOST] == Configurator.instance.hostname()
        continue
      end

      config_obj = Properties.new
      config_obj.props = @config.props.merge(host_props)
      
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def get_deployment_basedir(deployment_config)
    Configurator.instance.get_base_path()
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
    else
      false
    end
  end
  
  def get_default_config_filename
    Configurator.instance().get_base_path() + "/" + Configurator::HOST_CONFIG
  end
end