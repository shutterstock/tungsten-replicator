module ConfigurePromptInterface
  include ConfigureMessages
  
  COMMAND_HELP = "help"
  COMMAND_SAVE = "save"
  COMMAND_PREVIOUS = "prev"
  COMMAND_ACCEPT_DEFAULTS = "defaults"
  
  def set_config(config)
    @config = config
  end
  
  # Get the config key
  def get_name
    @name
  end
  
  def is_initialized?
    raise "Undefined function: is_initialized?"
  end
  
  def run
    raise "Undefined function: run"
  end
  
  def save_current_value
    raise "Undefined function: save_current_value"
  end
  
  def save_disabled_value
    raise "Undefined function: save_disabled_value"
  end
  
  def is_valid?
    raise "Undefined function: is_valid?"
  end
  
  def get_keys
    raise "Undefined function: get_keys"
  end
  
  def required?
    true
  end
  
  # Does the user need to answer this prompt
  def enabled?
    true
  end
  
  def allow_previous?
    enabled?()
  end
  
  # Get the sorting weight
  def get_weight
    @weight
  end
  
  # Get the value that should be set if this prompt is disabled
  def get_disabled_value
    nil
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
  
  # Build the help filename based on the config key
  def get_prompt_help_filename(prompt_name)
    "#{get_interface_text_directory()}/help_#{prompt_name}"
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