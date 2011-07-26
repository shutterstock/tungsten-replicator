class ConfigurePackage
  include ConfigureMessages
  
  def initialize(config)
    @config = config
  end
  
  def store_config_file?
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
  
  def post_prompt_handler_run
    true
  end
  
  def prepare_saved_config(config)
    config
  end
  
  def output_usage
    puts "Usage: configure [general-options]"
    output_general_usage()
  end
  
  def output_general_usage
    Configurator.instance.write_divider(Logger::ERROR)
    puts "General options:"
    output_usage_line("-a, --advanced", "Enabled advanced options")
    output_usage_line("-b, --batch", "Execute the configuration without interactive prompts")
    output_usage_line("-c, --config file", "Sets name of config file (default: tungsten.cfg)")
    output_usage_line("--config-file-help", "Display help information for content of the config file")
    output_usage_line("-f, --force", "Do not display confirmation prompts or stop the configure process for errors")
    output_usage_line("-h, --help", "Displays help message")
    output_usage_line("-q, --quiet", "Only display error messages")
    output_usage_line("-v, --verbose", "Display all messages")
    output_usage_line("--no-validation", "Skip all validation checks")
    output_usage_line("--validate-only", "Skip all deployment steps")
    output_usage_line("--configure=key=value", "Set the default configure script value for the given key to the value")
    output_usage_line("--property=key=value", "Set the property key to the value in any file that is modified by the configure script")
  end
end