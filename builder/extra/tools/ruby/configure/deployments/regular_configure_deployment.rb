class RegularConfigureDeployment < ConfigureDeployment
  def get_name
    "regular"
  end
  
  def get_deployment_configurations()
    config_objs = []
    
    if @config.getProperty(DEPLOY_PACKAGE_URI)
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))
      package_basename = File.basename(uri.path)
    else
      uri = nil
    end
    
    @config.getProperty(GLOBAL_HOSTS).split(",").each{
      |deployment_host|
      config_obj = Properties.new
      config_obj.props = @config.props.dup
      config_obj.setProperty(DSNAME, config_obj.getProperty(GLOBAL_DSNAME))
      config_obj.setProperty(GLOBAL_HOST, deployment_host)
      config_obj.setProperty(GLOBAL_IP_ADDRESS, Resolv.getaddress(deployment_host))
      
      if uri && uri.scheme == "file" && (uri.host == nil || uri.host == "localhost")
        config_obj.setProperty(GLOBAL_DEPLOY_PACKAGE_URI, @config.getProperty(DEPLOY_PACKAGE_URI))
        config_obj.setProperty(DEPLOY_PACKAGE_URI, "file://localhost#{config_obj.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}")
      end
      
      config_obj.getProperty(REPL_SERVICES).split(",").each{
        |service_name|
        service_config = config_obj.getProperty(Configurator::SERVICE_CONFIG_PREFIX + service_name)
        
        unless service_config
          raise "Unable to find service configuration for '#{service_name}'"
        end
      }
      
      config_objs.push(config_obj)
    }
    
    config_objs
  end
  
  def get_deployment_object_modules
    modules = [
      ConfigureDeploymentStepDeployment
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
end