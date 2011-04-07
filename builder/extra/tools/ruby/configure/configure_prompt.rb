# The parent class for each configure prompt.  It is responsible for
# collecting a value from the command line, validating it and placing it
# in the config object
class ConfigurePrompt
  include ConfigurePromptInterface

  def initialize(name, prompt, validator = nil, default = "")
    @name = name
    @prompt = prompt
    @validator = validator
    @default = default.to_s
    @config = nil
    @weight = 0
  end
  
  # Used to ensure that the basics have been set
  def is_initialized?
    if get_name() == nil || get_prompt() == nil
      false
    else
      true
    end
  end
  
  # Collect the value from the command line
  def run
    # Skip this prompt and remove the config value if this prompt isn't needed
    unless enabled?()
      save_disabled_value()
      return
    end
    
    puts
    Configurator.instance.write_divider
    
    description = get_description()
    unless description == nil
      puts description
    end
    
    puts
    value = nil
    while value == nil do
      begin
        # Display the prompt and collect the response
        raw_value = input_value(get_prompt(), get_value())
        
        case raw_value
        when COMMAND_HELP
          puts
          puts get_help()
          puts
        when COMMAND_PREVIOUS
          # Go back
          raise ConfigurePreviousPrompt
        when COMMAND_ACCEPT_DEFAULTS
          # Accept the default value for this and all remaining prompts
          raise ConfigureAcceptAllDefaults
        when COMMAND_SAVE
          # Save the current config values and exit
          raise ConfigureSaveConfigAndExit
        else
          # Validate the response against the prompt validation rule
          value = accept?(raw_value)
        end
      rescue PropertyValidatorException => e
        # Catch a prompt validation error and display the prompt again
        Configurator.instance.error(e.to_s)
      end
    end
    
    # Save the validated response to the config object
     @config.setProperty(get_name(), value)
  end
  
  # Get the current value for the prompt, use the default if the config does
  # not have a response for the given config key
  def get_value(allow_default = true)
    value = @config.getProperty(get_name())
    if value == nil && allow_default
      value = get_default_value()
    end
    
    value
  end
  
  def save_current_value
     @config.setProperty(get_name(), get_value())
  end
  
  def save_disabled_value
     @config.setProperty(get_name(), get_disabled_value())
  end
  
  # Ensure that the values in the config object pass all of the 
  def is_valid?
    value = get_value(false)
    
    if enabled?
      if value == nil && required?()
        # The prompt is enabled, the value should not be missing
        raise PropertyValidatorException.new("Value is missing")
      end
      
      if value != nil
        value = accept?(value.to_s())
      end
      
      # Validation passed
      true
    else
      if value.to_s() == ""
        if get_disabled_value() == nil
          # The prompt is disabled, no value should be given
          true
        elsif required?()
          raise PropertyValidatorException.new("Value is missing")
        end
      else
        if get_disabled_value() == nil
          # The prompt is disabled, the value should be empty
          raise PropertyValidatorException.new("Value should not be given, remove it from the configuration")
        else
          true
        end
      end
    end
  end
  
  def get_keys
    [@name]
  end
  
  # Get the prompt text
  def get_prompt
    @prompt
  end
  
  # Get the default value for the prompt
  def get_default_value
    @default
  end
end

module AdvancedPromptModule
  def enabled?
    Configurator.instance.advanced_mode?()
  end
  
  def get_disabled_value
    get_default_value()
  end
end

class AdvancedPrompt < ConfigurePrompt
  include AdvancedPromptModule
end

class ConstantValuePrompt < ConfigurePrompt
  def enabled?
    false
  end
  
  def get_disabled_value
    get_default_value()
  end
end

class InterfaceMessage < ConfigurePrompt
  def initialize(message_id, title = nil)
    @name = message_id
    @title = title
    @weight = 0
  end
  
  def is_initialized?
    true
  end
  
  def run
    unless enabled?()
      return
    end
    
    puts
    if @title == nil
      Configurator.instance.write_divider
    else
      Configurator.instance.write_header(@title)
    end
    
    puts get_description()
  end
  
  def is_valid?
    true
  end
  
  def allow_previous?
    false
  end
end

class AdvancedInterfaceMessage < InterfaceMessage
  def enabled?
    Configurator.instance.advanced_mode?()
  end
end