# The parent class for each configure prompt.  It is responsible for
# collecting a value from the command line, validating it and placing it
# in the config object
class ConfigurePrompt
  include ConfigureMessages
  COMMAND_HELP = "help"
  COMMAND_SAVE = "save"
  COMMAND_PREVIOUS = "prev"
  COMMAND_ACCEPT_DEFAULTS = "defaults"

  def initialize(name, prompt, validator = nil, default = "")
    @name = name
    @prompt = prompt
    @weight = 0
    @validator = validator
    @default = default.to_s
    @config = nil
  end
  
  # Used to ensure that the basics have been set
  def is_initialized
    if get_name() == nil || get_prompt() == nil
      false
    else
      true
    end
  end
  
  def set_config(config)
    @config = config
  end
  
  # Collect the value from the command line
  def run
    # Skip this prompt and remove the config value if this prompt isn't needed
    unless enabled?()
      save_value(get_disabled_value())
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
        raw_value = input_value(get_prompt, get_value())
        
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
    save_value(value)
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
  
  # Save the response into the config object
  def save_value(value)
    @config.setProperty(get_name(), value)
  end
  
  # Read the prompt response from the command line.
  def input_value(prompt, default)
    default = default.to_s
    if (default.length + prompt.length < 75)
      printf "%s [%s]: ", prompt, default
    else
      printf "%s\n[%s]:", prompt, default
    end
    value = STDIN.gets
    value.strip!
    if value == ""
      return default
    else
      return value
    end
  end
  
  # Validate the response against the prompt validation rules
  def accept?(raw_value)
    if @validator
      @validator.validate raw_value
    else
      raw_value
    end
  end
  
  # Ensure that the values in the config object pass all of the 
  def is_valid?
    value = get_value(false)
    
    if enabled?
      if value == nil
        # The prompt is enabled, the value should not be missing
        raise PropertyValidatorException.new("Value is missing")
      end
      
      value = accept?(value)
      if value == nil
        # There was an issue in the validation
        raise PropertyValidatorException.new("Value does not pass validation")
      else
        # Validation passed
        true
      end
    else
      if value == nil || value == ""
        if get_disabled_value() == nil
          # The prompt is disabled, no value should be given
          true
        else
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
  
  # Get the config key
  def get_name
    @name
  end
  
  # Get the prompt text
  def get_prompt
    @prompt
  end

  # Get the sorting weight
  def get_weight
    @weight
  end
  
  # Get the default value for the prompt
  def get_default_value
    @default
  end
  
  # Does the user need to answer this prompt
  def enabled?
    true
  end
  
  # Get the value that should be set if this prompt is disabled
  def get_disabled_value
    nil
  end

  def allow_previous?
    enabled?()
  end
  
  # Get the help text for this prompt
  def get_help
    help = get_prompt_help(get_name())
    
    if help == nil || help == ""
      help = get_description()
    end
    
    if help == nil || help == ""
      help = "Sorry, no help is available."
    end
    
    help
  end
  
  # Build the help filename based on the config key
  def get_prompt_help_filename(prompt_name)
    "#{get_interface_text_directory()}/help_#{prompt_name}"
  end
  
  # Read the help from the prompt help file
  def get_prompt_help(prompt_name)
    help_filename = get_prompt_help_filename(prompt_name)
    unless File.exists?(help_filename)
      return nil
    end
    
    help = ''
    f = File.open(help_filename, "r") 
    f.each_line do |line|
      help += line
    end
    f.close
    
    return help
  end
  
  def get_description
    get_prompt_description(get_name())
  end
  
  # Build the description filename based on the config key
  def get_prompt_description_filename(prompt_name)
    "#{get_interface_text_directory()}/prompt_#{prompt_name}"
  end
  
  def get_interface_text_directory
    "#{Configurator.instance.get_base_path()}/#{Configurator.instance.get_ruby_prefix()}/configure/interface_text"
  end
  
  # Read the help from the prompt help file
  def get_prompt_description(prompt_name)
    description_filename = get_prompt_description_filename(prompt_name)
    unless File.exists?(description_filename)
      return nil
    end
    
    description = ''
    f = File.open(description_filename, "r") 
    f.each_line do |line|
      description += line
    end
    f.close
    
    return description
  end
  
  def cmd_result(command, ignore_fail = false)
    debug("Execute `#{command}`")
    result = `#{command} 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise "Failed: #{command}, RC: #{rc}, Result: #{result}"
    else
      debug("RC: #{rc}, Result: #{result}")
    end
    
    return result
  end
  
  def ssh_result(command, ignore_fail = false, host = nil, user = nil)
    if (user == nil)
      user = @config.getProperty(GLOBAL_USERID)
    end
    if (host == nil)
      host = @config.getProperty(GLOBAL_HOST)
    end
    
    debug("Execute `#{command}` on #{host}")
    result = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise "Failed: #{command}, RC: #{rc}, Result: #{result}"
    else
      debug("RC: #{rc}, Result: #{result}")
    end
    
    return result
  end
end

class AdvancedPrompt < ConfigurePrompt
  def enabled?
    Configurator.instance.advanced_mode?()
  end
  
  def get_disabled_value
    get_default_value()
  end
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
  
  def is_initialized
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