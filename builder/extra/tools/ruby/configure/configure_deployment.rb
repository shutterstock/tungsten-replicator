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
    
    repl_services = []
    expanded_config.setDefault(GLOBAL_BASEDIR, get_deployment_basedir(deployment_config))
    
    ClusterConfigureModule.each_service(deployment_config) {
      |parent_name,service_name,service_properties|
      
      service_hosts = service_properties[REPL_HOSTS].to_s().split(",")
      remote_hosts = service_properties[REPL_REMOTE_HOSTS].to_s().split(",")
      if (service_hosts.include?(expanded_config.getProperty(GLOBAL_HOST)) ||
          remote_hosts.include?(expanded_config.getProperty(GLOBAL_HOST)) ||
          service_properties[REPL_MASTERHOST] == expanded_config.getProperty(GLOBAL_HOST))
        repl_services << service_name
      else
        next
      end
      
      expanded_config.setDefault([parent_name, REPL_SVC_CONFIG_FILE], 
        "#{expanded_config.getProperty(GLOBAL_BASEDIR)}/tungsten-replicator/conf/static-#{service_name}.properties")
        
      if expanded_config.getProperty([parent_name, REPL_REMOTE_HOSTS]) && 
          expanded_config.getProperty([parent_name, REPL_REMOTE_HOSTS]).split(",").include?(expanded_config.getProperty(GLOBAL_HOST))
        expanded_config.setProperty([parent_name, REPL_SVC_SERVICE_TYPE], "remote")
      else
        expanded_config.setProperty(GLOBAL_DSNAME, service_name)
        expanded_config.setProperty([parent_name, REPL_SVC_SERVICE_TYPE], "local")
      end
      
      if expanded_config.getProperty([parent_name, REPL_MASTERHOST]) == expanded_config.getProperty(GLOBAL_HOST)
        expanded_config.setProperty([parent_name, REPL_ROLE], "master")
      else
        expanded_config.setProperty([parent_name, REPL_ROLE], "slave")
      end
      
      deployment_config.props.each{
        |key,value|
        
        if value.is_a?(Hash)
          next
        end
        
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
    
    expanded_config.setProperty(REPL_SERVICES, repl_services.join(","))
    expanded_config
  end
  
  def get_deployment_basedir(deployment_config)
    "#{deployment_config.getProperty(GLOBAL_HOME_DIRECTORY)}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
  end
  
  def validate
    get_validation_handler().run(get_deployment_configurations())
  end
  
  def validate_config(deployment_config)
    expanded_config = expand_deployment_configuration(deployment_config)
    @config.props = expanded_config.props
    get_validation_handler().validate_config()
  end
  
  def deploy
    get_deployment_handler().run(get_deployment_configurations())
  end
  
  def deploy_config(deployment_config)
    expanded_config = expand_deployment_configuration(deployment_config)
    @config.props = expanded_config.props
    get_deployment_handler().deploy_config()
  end
  
  def get_validation_handler_class
    ConfigureValidationHandler
  end
  
  def get_validation_handler
    unless @validation_handler
      @validation_handler = get_validation_handler_class().new(@config)
    end
    
    @validation_handler
  end
  
  def get_deployment_handler_class
    ConfigureDeploymentHandler
  end
  
  def get_deployment_handler
    unless @deployment_handler
      @deployment_handler = get_deployment_handler_class().new(@config)
    end
    
    @deployment_handler
  end
  
  def get_deployment_object
    # Load each of the files in the deployement_steps directory
    Dir[File.dirname(__FILE__) + '/deployment_steps/*.rb'].each do |file| 
      system_require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    obj = Class.new{
      include ConfigureDeploymentCore
    }.new(@config)
    
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
    obj
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