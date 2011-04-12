class MultipleValueConfigurePrompt
  include ConfigurePromptInterface
  
  def initialize(source_name, parent_name_prefix, name, prompt, 
      validator = nil, default = nil)
    @source_name = source_name
    @parent_name_prefix = parent_name_prefix
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
    each_source{
      |parent_name,source_val|
      value = nil
      while value == nil do
        begin
          # Display the prompt and collect the response
          raw_value = input_value(get_prompt(source_val), get_value(parent_name))

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
          if raw_value == "" && !required?()
            value = nil
            break
          end
          # Catch a prompt validation error and display the prompt again
          Configurator.instance.error(e.to_s)
        end
      end

      # Save the validated response to the config object
      @config.setNestedProperty(value, parent_name, get_name())
    }
  end
  
  def enabled?
    (@config.getProperty(@source_name).to_s() != "")
  end
  
  # Get the prompt text
  def get_prompt(source_val = "")
    @prompt.sub('@value', source_val)
  end
  
  # Get the default value for the prompt
  def get_default_value
    @default
  end
  
  def get_keys
    keys = []
    each_source{
      |parent_name,source_val|
      keys << "#{parent_name}.#{get_name()}"
    }
    
    return keys
  end
  
  def get_value(parent_name)
    @config.getPropertyOr([parent_name, get_name()], get_default_value())
  end
  
  def save_current_value
    each_source{
      |parent_name,source_val|
      @config.setProperty([parent_name, get_name()], get_value(parent_name))
    }
  end
  
  def save_disabled_value
    each_source{
      |parent_name,source_val|
      @config.setProperty([parent_name, get_name()], get_disabled_value())
    }
  end
  
  # Ensure that the values in the config object pass all of the 
  def is_valid?
    errors = []
    
    each_source{
      |parent_name,source_val|
      value = @config.getNestedProperty(parent_name, get_name())
      
      if enabled?
        if value == nil && required?()
          # The prompt is enabled, the value should not be missing
          errors << ConfigurePromptError.new(
            ConfigurePrompt.new("#{parent_name}.#{get_name()}", get_prompt(source_val)),
            "Value is missing", "")
        elsif value != nil
          begin
            value = accept?(value.to_s())
          rescue Exception => e
            # There was an issue in the validation
            errors << ConfigurePromptError.new(
              ConfigurePrompt.new("#{parent_name}.#{get_name()}", get_prompt(source_val)),
              e.to_s(), value)
          end
        end
      else
        if value.to_s() == ""
          if get_disabled_value() == nil
            # The prompt is disabled, no value should be given
          elsif required?()
            errors << ConfigurePromptError.new(
              ConfigurePrompt.new("#{parent_name}.#{get_name()}", get_prompt(source_val)),
              "Value is missing", "")
          end
        else
          if get_disabled_value() == nil
            # The prompt is disabled, the value should be empty
            errors << ConfigurePromptError.new(
              ConfigurePrompt.new("#{parent_name}.#{get_name()}", get_prompt(source_val)),
              "Value should not be given, remove it from the configuration", value)
          end
        end
      end
    }
    
    if errors.length > 0
      raise ConfigurePromptErrorSet.new(errors)
    else
      true
    end
  end
  
  def each_source(&block)
    @config.getPropertyOr(@source_name, "").split(",").each{
      |source_val|
      block.call(@parent_name_prefix + source_val, source_val)
    }
  end
end