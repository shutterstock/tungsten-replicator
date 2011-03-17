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
  
  def validate
    get_validation_handler().run(get_deployment_configurations())
  end
  
  def validate_config(config)
    @config.props = config.props
    get_validation_handler().validate_config()
  end
  
  def deploy
    get_deployment_handler().run(get_deployment_configurations())
  end
  
  def deploy_config(config)
    @config.props = config.props
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