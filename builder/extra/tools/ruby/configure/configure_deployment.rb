class ConfigureDeployment
  FINAL_STEP_WEIGHT = 10000
  
  def initialize
    @config = Properties.new()
    @validation_handler = nil
    @deployment_handler = nil
  end
  
  def set_config(config)
    @config.props = config.props.dup()
  end
  
  def get_name
    raise "This function must be overwritten"
  end
  
  def get_deployment_configurations
    raise "This function must be overwritten"
  end
  
  def expand_deployment_configuration(deployment_config)
    expanded_config = deployment_config.dup()
    
    expanded_config.setDefault(BASEDIR, get_deployment_basedir(expanded_config))

    hostname = Configurator.instance.hostname()
    unless expanded_config.getProperty(DEFAULT_DATASERVER)
      default_ds = nil
      expanded_config.getPropertyOr(DATASERVERS, {}).each{
        |ds_alias, ds_props|
        
        if ds_props[REPL_DBHOST] == hostname
          default_ds = ds_alias
          break
        end
      }
      
      unless default_ds
        raise "Unable to determine the default dataserver"
      end
      
      expanded_config.setProperty(DEFAULT_DATASERVER, default_ds)
    end
    
    repl_services = []
    ClusterConfigureModule.each_service(deployment_config) {
      |parent_name,service_name,service_properties|
      
      expanded_config.setDefault([parent_name, REPL_SVC_CONFIG_FILE], 
        "#{expanded_config.getProperty(BASEDIR)}/tungsten-replicator/conf/static-#{service_name}.properties")
        
      if ClusterConfigureModule.services_list(expanded_config).include?(expanded_config.getProperty(HOST))
        expanded_config.setProperty([parent_name, REPL_SVC_SERVICE_TYPE], "remote")
      else
        expanded_config.setProperty([parent_name, REPL_SVC_SERVICE_TYPE], "local")
      end
      
      case expanded_config.getProperty([parent_name, REPL_SVC_MODE])
      when REPL_MODE_MS
        if expanded_config.getProperty([parent_name, REPL_MASTERHOST]) == expanded_config.getProperty(HOST)
          expanded_config.setProperty([parent_name, REPL_ROLE], "master")
        else
          expanded_config.setProperty([parent_name, REPL_ROLE], "slave")
        end
        
        service_hosts = service_properties[REPL_HOSTS].to_s().split(",")
        remote_hosts = service_properties[REPL_REMOTE_HOSTS].to_s().split(",")
        if (service_hosts.include?(expanded_config.getProperty(HOST)) ||
            remote_hosts.include?(expanded_config.getProperty(HOST)) ||
            service_properties[REPL_MASTERHOST] == expanded_config.getProperty(HOST))
          repl_services << service_name
        end
      when REPL_MODE_DI
        expanded_config.setProperty([parent_name, REPL_ROLE], "direct")
        expanded_config.setDefault([parent_name, REPL_MASTERHOST], expanded_config.getProperty(HOST))
        expanded_config.setDefault([parent_name, REPL_SVC_THL_PORT], "2112")
        expanded_config.setDefault([parent_name, REPL_HOSTS], expanded_config.getProperty(HOST))
        repl_services << service_name
      else
        raise "Unable to determine the replication role based on replication mode: #{expanded_config.getProperty([parent_name, REPL_SVC_MODE])}"
      end
      
      deployment_config.props.each{
        |key,value|
        
        if value.is_a?(Hash)
          next
        end
        
        # Merge global properties into the service configuration but do not 
        # overwriting existing properties
        case key
        when REPL_SERVICES then
          next
        when REPL_LOG_DIR then
          expanded_config.setDefault([parent_name, key], "#{value}/#{service_name}")
        when REPL_RELAY_LOG_DIR then
          expanded_config.setDefault([parent_name, key], "#{value}/#{service_name}")
        else
          expanded_config.setDefault([parent_name, key], value)
        end
      }
    }
    
    # The replication services that are enabled on this host
    expanded_config.setProperty(REPL_SERVICES, repl_services.join(","))
    expanded_config
  end
  
  def get_deployment_basedir(deployment_config)
    "#{deployment_config.getProperty(HOME_DIRECTORY)}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
  end
  
  def validate
    get_validation_handler().run(get_deployment_configurations())
  end
  
  def validate_config(deployment_config)
    expanded_config = expand_deployment_configuration(deployment_config)
    get_validation_handler().validate_config(expanded_config)
  end
  
  def deploy
    get_deployment_handler().run(get_deployment_configurations())
  end
  
  def deploy_config(deployment_config)
    # Load each of the files in the deployement_steps directory
    Dir[File.dirname(__FILE__) + '/deployment_steps/*.rb'].each do |file| 
      system_require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    expanded_config = expand_deployment_configuration(deployment_config)
    
    # Get an object that represents the deployment steps required by the config
    obj = Class.new{
      include ConfigureDeploymentCore
    }.new(expanded_config)

    deployment_methods = []
    get_deployment_object_modules().each{
      |module_name|
      obj.extend(module_name)
      begin
        deployment_methods = deployment_methods + module_name.get_deployment_methods()
      rescue
      end
    }

    obj.set_deployment_methods(deployment_methods)

    # Execute each of the deployment steps
    obj.deploy()
  end
  
  def get_validation_handler_class
    ConfigureValidationHandler
  end
  
  def get_validation_handler
    unless @validation_handler
      @validation_handler = get_validation_handler_class().new()
    end
    
    @validation_handler
  end
  
  def get_deployment_handler_class
    ConfigureDeploymentHandler
  end
  
  def get_deployment_handler
    unless @deployment_handler
      @deployment_handler = get_deployment_handler_class().new(self)
    end
    
    @deployment_handler
  end
  
  def get_deployment_object_modules
    []
  end
  
  def get_weight
    0
  end
  
  def include_deployment_for_package?(package)
    false
  end
  
  def get_default_config_filename
    Configurator.instance().get_base_path() + "/" + Configurator::CLUSTER_CONFIG
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses
  end
end