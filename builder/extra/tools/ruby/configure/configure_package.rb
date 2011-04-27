class ConfigurePackage
  attr_accessor :remainder_option_property
  
  def initialize(config)
    @config = config
    @remainder_option_property = nil
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
  
  def prepare_parser(opts)
  end
  
  def allowed_property_options
    {}
  end
  
  def output_usage
    puts "Usage: configure [options]"
    output_general_usage()
  end
  
  def output_general_usage
    Configurator.instance.write_divider
    puts "General options:"
    puts "-h, --help         Displays help message"
    puts "-a, --advanced     Enable advanced options"
    puts "-b, --batch        Batch execution from existing config file"
    puts "-c, --config file  Sets name of config file (default: tungsten.cfg)"
    puts "-f, --force        Skip validation checks"
    puts "-p, --package      Class name for the configuration package"
    puts "-q, --quiet        Quiet output"
    puts "-v, --verbose      Verbose output"
    puts "--validate-only    Do not execute the deployment"
  end
  
  def expand_deployment_configuration(config)
    config.dup()
  end
end