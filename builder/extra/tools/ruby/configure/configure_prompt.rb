# The parent class for each configure prompt.  It is responsible for
# collecting a value from the command line, validating it and placing it
# in the config object
class ConfigurePrompt
  include ConfigurePromptInterface
  @@global_defaults = {}
  
  def self.add_global_default(key, value)
    if value == nil
      raise("Unable to make a global replacement using a nil value")
    end
    
    @@global_defaults[key] = value
  end
  
  def self.get_global_default(key)
    if @@global_defaults.has_key?(key)
      return @@global_defaults[key]
    else
      return nil
    end
  end

  def initialize(name, prompt, validator = nil, default = nil)
    @name = name
    @prompt = prompt
    @validator = validator
    @default = default
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
    unless enabled_for_config?()
      save_disabled_value()
    end
    
    unless enabled?
      return
    end
    
    description = get_description()
    unless description == nil
      puts
      Configurator.instance.write_divider
      puts description
      puts
    end
    
    value = get_input_value()
    
    # Save the validated response to the config object
    if value != get_default_value()
      @config.setProperty(get_name(), value)
    else
      @config.setProperty(get_name(), nil)
    end
  end
  
  def get_input_value
    value = nil
    while value == nil do
      begin
        # Display the prompt and collect the response
        raw_value = input_value(get_display_prompt(), get_value())
        
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
    
    value
  end
  
  # Get the current value for the prompt, use the default if the config does
  # not have a response for the given config key
  def get_value(allow_default = true, allow_disabled = false)
    value = @config.getProperty(get_name(), allow_disabled)
    if value == nil && allow_default && (enabled_for_config?() || allow_disabled)
      global_default = ConfigurePrompt.get_global_default(@name)
      
      if global_default == nil
        value = get_default_value()
      else
        begin
          value = accept?(global_default)
        rescue
          value = global_default
        end
      end
    end
        
    value
  end
  
  def get_template_value(transform_values_method)
    get_value()
  end
  
  # Save the current value back to the config object or the default 
  # value if none is set
  def save_current_value
    if (v = get_value()) != get_default_value()
      @config.setProperty(get_name(), v)
    else
      @config.setProperty(get_name(), nil)
    end
  end

  # Save the disabled value back to the config object
  def save_disabled_value
    if (v = get_disabled_value()) != get_default_value()
      @config.setProperty(get_name(), v)
    else
      @config.setProperty(get_name(), nil)
    end
  end
  
  # Get the value that should be set if this prompt is disabled
  def get_disabled_value
    nil
  end
  
  def is_valid?
    if enabled_for_config?
      value = get_value()
      if value == nil && required?()
        # The prompt is enabled, the value should not be missing
        raise ConfigurePromptError.new(
          self.clone(),
          "Value is missing", "")
      elsif value != nil
        begin
          value = accept?(value.to_s())
        rescue Exception => e
          # There was an issue in the validation
          raise ConfigurePromptError.new(
            self.clone(),
            e.to_s(), value)
        end
      end
    else
      value = get_value(false)
      if value.to_s() == ""
        if get_disabled_value() == nil
          # The prompt is disabled, no value should be given
        elsif required?()
          raise ConfigurePromptError.new(
            self.clone(),
            "Value is missing", "")
        end
      else
        if get_disabled_value() == nil
          # The prompt is disabled, the value should be empty
          raise ConfigurePromptError.new(
            self.clone(),
            "Value should not be given, remove it from the configuration", value)
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
  
  def get_display_prompt
    get_prompt()
  end
  
  # Get the default value for the prompt
  def get_default_value
    if @default
      @default.to_s
    else
      nil
    end
  end
  
  def get_property(attrs, allow_disabled = false)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size > 1
      raise "Unable to get_property:#{attrs.join('.')} for #{self.class.name}"
    end
    
    value = get_value(true, allow_disabled)    
    if @validator.is_a?(FilePropertyValidator) || @validator == PV_FILENAME
      value = format_filename_property(value)
    end
    
    return value
  end
  
  def format_filename_property(value)
    begin
      if value.to_s == ""
        return value
      end
      
      @cache ||= {}
      userid = get_userid()
      host = get_hostname()
      
      if userid == nil || host == nil
        raise IgnoreError
      end
      
      cache_key = "#{userid}@#{host}:#{value}"
      unless @cache.has_key?(cache_key)
        @cache[cache_key] = ssh_result("echo #{value}", host, userid)
      end
      
      value = @cache[cache_key]
    rescue
    end
    
    return value
  end
  
  def find_template_value(attrs, transform_values_method)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size > 1
      raise "Unable to find_template_value:#{attrs.join('.')} for #{self.class.name}"
    end
    
    get_template_value(transform_values_method)
  end
  
  def replace_deprecated_key(deprecated_key)
    if (v = @config.getProperty(deprecated_key)) != nil
      @config.setProperty(get_name(), v)
      @config.setProperty(deprecated_key, nil)
    end
  end
end

module AdvancedPromptModule
  def enabled?
    super() && Configurator.instance.advanced_mode?()
  end
  
  def enabled_for_command_line?
    super() && Configurator.instance.advanced_mode?()
  end
  
  def enabled_for_config?
    true
  end
  
  def get_disabled_value
    if enabled_for_config?
      get_value()
    else
      super()
    end
  end
end

module ConstantValueModule
  def enabled?
    false
  end
  
  def enabled_for_config?
    true
  end
  
  def get_disabled_value
    get_value()
  end
  
  def enabled_for_command_line?()
    false
  end
  
  def output_config_file_usage
  end
end

module HiddenValueModule
  include ConstantValueModule
  
  def enabled_for_template_file?
    false
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

class TemporaryPrompt < ConfigurePrompt
  def initialize(prompt, validator = nil, default = "")
    super("", prompt, validator, default)
    @config = Properties.new
  end
  
  def is_initialized?
    if get_prompt() == nil
      false
    else
      true
    end
  end
  
  # Collect the value from the command line
  def run
    # Skip this prompt and remove the config value if this prompt isn't needed
    unless enabled?()
      return
    end
    
    description = get_description()
    unless description == nil
      puts
      Configurator.instance.write_divider
      puts description
      puts
    end
    
    get_input_value()
  end
  
  def get_name
    ""
  end
  
  def get_keys
    []
  end
  
  def save_current_value
  end

  # Save the disabled value back to the config object
  def save_disabled_value
  end
end