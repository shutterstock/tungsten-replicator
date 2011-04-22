class RegularConfigureDeployment < ConfigureDeployment
  def get_name
    "distributed"
  end
  
  def get_deployment_configurations()
    config_objs = []
    hosts = []
    
    if @config.getProperty(DEPLOY_PACKAGE_URI)
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))
      package_basename = File.basename(uri.path)
    else
      uri = nil
    end
    
    @config.getProperty(GLOBAL_HOSTS).each{
      |host_alias, host_props|

      config_obj = Properties.new
      config_obj.props = @config.props.merge(host_props)
      
      if uri && uri.scheme == "file" && (uri.host == nil || uri.host == "localhost")
        config_obj.setProperty(GLOBAL_DEPLOY_PACKAGE_URI, @config.getProperty(DEPLOY_PACKAGE_URI))
        config_obj.setProperty(DEPLOY_PACKAGE_URI, "file://localhost#{config_obj.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}")
      end
    
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepDeployment
      ]
    
    modules << ConfigureDeploymentStepReplicationDataservice
    case @config.getProperty(GLOBAL_DBMS_TYPE)
    when "mysql"
      modules << ConfigureDeploymentStepMySQL
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
end