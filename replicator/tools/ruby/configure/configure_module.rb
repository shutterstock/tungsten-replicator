# The parent class for each ConfigureModule that groups the prompts,
# validation checks and deployment steps
class ConfigureModule
  def initialize
    @config = nil
    @weight = 0
  end
  
  def get_weight
    @weight
  end
  
  def register_prompts(prompt_handler)
  end
  
  def register_validation_checks(validation_handler)
  end
  
  def set_config(config)
    @config = config
  end
  
  def include_module_for_package?(package)
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