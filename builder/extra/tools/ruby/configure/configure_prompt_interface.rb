module ConfigurePromptInterface
  include ConfigureMessages
  attr_accessor :name
  
  COMMAND_HELP = "help"
  COMMAND_SAVE = "save"
  COMMAND_PREVIOUS = "prev"
  COMMAND_ACCEPT_DEFAULTS = "defaults"
  
  # Set the config object that this prompt should modify when saving values
  def set_config(config)
    @config = config
  end
  
  # The config hash key for this prompt
  def get_name
    @name
  end
  
  # The argument name that will be used to set this prompt from 
  # the command line
  def get_command_line_argument()
    @name.gsub("_", "-")
  end
  
  # The value to set when the command line argument is present
  # If this is not nil, no value will be accepted from the command line
  def get_command_line_argument_value
    nil
  end
  
  # Are all class variables specified
  def is_initialized?
    raise "Undefined function: is_initialized?"
  end
  
  # Interact with the user to get the value for this prompt
  def run
    raise "Undefined function: run"
  end
  
  def save_current_value
    raise "Undefined function: save_current_value"
  end
  
  def save_disabled_value
    raise "Undefined function: save_disabled_value"
  end
  
  # Check that the value currently in the config object is valid
  def is_valid?
    raise "Undefined function: is_valid?"
  end
  
  # Get the list of config hash keys that are allowed for this prompt
  def get_keys
    raise "Undefined function: get_keys"
  end
  
  def required?
    enabled?()
  end
  
  # Does the user need to answer this prompt
  def enabled?
    true
  end
  
  # Is this value allowed in the config file
  # If a prompt is not needed based on other responses, we do not let 
  # them set the value in the config to avoid confusion
  def enabled_for_config?
    enabled?()
  end
  
  # Is this value accepted from the command line
  def enabled_for_command_line?()
    true
  end
  
  # Is the value allowed to be used in template files
  def enabled_for_template_file?()
    true
  end
  
  # Return to this prompt if the user specifies 'previous' during
  # interactive configuration.
  def allow_previous?
    enabled?()
  end
  
  # Get the sorting weight
  def get_weight
    @weight || 0
  end
  
  # Read the prompt response from the terminal
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
  def get_prompt_help_filename()
    "#{get_interface_text_directory()}/help_#{get_name()}"
  end
  
  # Get the help text for this prompt
  def get_help
    help = get_prompt_help()
    
    if help == nil || help == ""
      help = get_description()
    end
    
    if help == nil || help == ""
      help = "Sorry, no help is available."
    end
    
    help
  end
  
  # Read the help from the prompt help file
  def get_prompt_help()
    help_filename = get_prompt_help_filename()
    unless File.exists?(help_filename)
      return nil
    end
    
    help = ''
    f = File.open(help_filename, "r") 
    f.each_line do |line|
      help += line.gsub(/\n/," ").scan(/\S.{0,#{70-2}}\S(?=\s|$)|\S+/).join("\n") + "\n"
    end
    f.close
    
    return help
  end
  
  # A wrapper function for get_prompt_description
  def get_description
    get_prompt_description()
  end
  
  # Build the description filename based on the config key
  def get_prompt_description_filename()
    "#{get_interface_text_directory()}/prompt_#{get_name()}"
  end
  
  def get_interface_text_directory
    "#{Configurator.instance.get_base_path()}/#{Configurator.instance.get_ruby_prefix()}/configure/interface_text"
  end
  
  # Read the help from the prompt help file
  def get_prompt_description()
    description_filename = get_prompt_description_filename()
    unless File.exists?(description_filename)
      return nil
    end
    
    description = ''
    f = File.open(description_filename, "r") 
    f.each_line do |line|
      description += line.gsub(/\n/," ").scan(/\S.{0,#{70-2}}\S(?=\s|$)|\S+/).join("\n") + "\n"
    end
    f.close
    
    return description
  end
  
  # Output how to set this value from the command line
  def output_usage
    output_usage_line("--#{get_command_line_argument()}", get_prompt(), get_value(true, true), nil, get_prompt_description())
  end
  
  # Output how to specify this value in a config file
  def output_config_file_usage
    output_usage_line(get_config_file_usage_symbol(), get_prompt(), get_default_value())
  end
  
  # The config hash key to output in output_config_file_usage
  def get_config_file_usage_symbol
    get_name()
  end
  
  # Output how to specify this value in a template file
  def output_template_file_usage
    output_usage_line(get_template_file_usage_symbol(), get_prompt())
  end
  
  # The template parameter to output in output_template_file_usage
  def get_template_file_usage_symbol
    Configurator.instance.get_constant_symbol(@name)
  end
  
  def each_prompt(&block)
    block.call(self)
    self
  end
  
  # Update the config object so that old values are set to their new keys
  def update_deprecated_keys()
  end
  
  def get_userid
    nil
  end
  
  def get_hostname
    nil
  end
end

module NoTemplateValuePrompt
  def enabled_for_template_file?()
    false
  end
end