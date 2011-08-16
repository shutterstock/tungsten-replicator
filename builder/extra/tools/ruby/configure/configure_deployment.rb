class ConfigureDeployment
  FINAL_STEP_WEIGHT = 10000
  
  def initialize
    @config = Properties.new()
    @validation_handler = nil
    @deployment_handler = nil
  end
  
  def set_config(config)
    @config = config
  end
  
  def get_name
    raise "This function must be overwritten"
  end
  
  def get_deployment_configurations
    raise "This function must be overwritten"
  end
  
  def expand_deployment_configuration(deployment_config)
    config = deployment_config.dup()
    
    config.getPropertyOr(REPL_SERVICES, {}).delete_if{
      |s_alias, s_props|
      
      (config.getProperty([REPL_SERVICES, s_alias, DEPLOYMENT_HOST]) != config.getProperty(DEPLOYMENT_HOST))
    }
    
    config.getPropertyOr(HOSTS, {}).delete_if{
      |h_alias, h_props|
      
      (h_alias != config.getProperty(DEPLOYMENT_HOST))
    }
    
    config
  end
  
  def get_deployment_basedir(config)
    config.getProperty(CURRENT_RELEASE_DIRECTORY)
  end
  
  def prevalidate
    get_validation_handler().prevalidate(get_deployment_configurations())
  end
  
  def validate
    get_validation_handler().validate(get_deployment_configurations())
  end
  
  def validate_config(deployment_config)
    expanded_config = expand_deployment_configuration(deployment_config)
    get_validation_handler().validate_config(expanded_config)
  end
  
  def prepare
    get_deployment_handler().prepare(get_deployment_configurations())
  end
  
  def deploy
    get_deployment_handler().deploy(get_deployment_configurations())
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
    get_deployment_object_modules(expanded_config).each{
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
  
  def cleanup
    get_deployment_handler().cleanup(get_deployment_configurations())
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
  
  def get_deployment_object_modules(config)
    []
  end
  
  def get_weight
    0
  end
  
  def include_deployment_for_package?(package)
    false
  end
  
  def require_package_uri
    false
  end
  
  def require_deployment_host
    false
  end
  
  def self.inherited(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses
  end
end

module DatabaseTypeDeploymentStep
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end