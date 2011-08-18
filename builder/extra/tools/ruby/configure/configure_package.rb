class ConfigurePackage
  include ConfigureMessages
  
  def initialize(config)
    @config = config
  end
  
  def store_config_file?
    false
  end
  
  def read_config_file?
    if Configurator.instance.is_interactive?()
      return true
    end
    
    if Configurator.instance.is_batch?()
      return true
    end
    
    false
  end
  
  def allow_interactive?
    true
  end
  
  def allow_batch?
    true
  end
  
  def get_non_interactive_prompts
    []
  end
  
  def get_prompts
    []
  end
  
  def get_validation_checks
    []
  end
  
  def parsed_options?(arguments)
    true
  end
  
  def output_usage
    puts "Usage: configure [general-options]"
    output_general_usage()
  end
  
  def output_general_usage
    Configurator.instance.write_divider(Logger::ERROR)
    puts "General options:"
    output_usage_line("-a, --advanced", "Enabled advanced options")
    output_usage_line("-b, --batch", "Execute the configuration without interactive prompts or command line arguments")
    output_usage_line("-c, --config file", "Sets name of config file (default: tungsten.cfg)")
    output_usage_line("-f, --force", "Do not display confirmation prompts or stop the configure process for errors")
    output_usage_line("-h, --help", "Displays help message")
    output_usage_line("-q, --quiet", "Only display error messages")
    output_usage_line("-v, --verbose", "Display all messages")
    output_usage_line("--net-ssh-option=key=value", "Set the Net::SSH option for remote system calls", nil, nil, "Valid options can be found at http://net-ssh.github.com/ssh/v2/api/classes/Net/SSH.html#M000002")
    
    if Configurator.instance.advanced_mode?()
      output_usage_line("--config-file-help", "Display help information for content of the config file")
      output_usage_line("--template-file-help", "Display the keys that may be used in configuration template files")
      output_usage_line("--no-validation", "Skip all validation checks")
      output_usage_line("--validate-only", "Skip all deployment steps")
      output_usage_line("--configure=key=value", "Set the default configure script value for the given key to the value")
      output_usage_line("--property=key=value", "Set the property key to the value in any file that is modified by the configure script")
    end
  end
end