class UpdatePackage < ConfigurePackage
  def get_prompts
    ConfigurePackageCluster.new(@config).get_prompts() + [
    ]
  end
  
  def get_validation_checks
    ConfigurePackageCluster.new(@config).get_validation_checks() + [
    ]
  end
  
  def parsed_options?(arguments)
    if Configurator.instance.display_help? && !Configurator.instance.display_preview?()
      return true
    end
    
    arguments = load_target_config(arguments)
    
    unless Configurator.instance.display_preview?
      unless @config.props.size > 0
        error("Unable to run update because this directory has not been setup")
        return false
      end
    end
    
    @config.setProperty(DEPLOYMENT_TYPE, UPDATE_DEPLOYMENT_NAME)
    
    host_alias = @config.getPropertyOr(HOSTS).keys.at(0)
    host_options = Properties.new()
    
    opts = OptionParser.new
    each_host_prompt{
      |prompt|
      opts.on("--#{prompt.get_command_line_argument()} String") {
        |val|
        host_options.setProperty(prompt.name, val)
      }
    }
    
    remainder = Configurator.instance.run_option_parser(opts, arguments)
    
    host_options.props.each{
      |h_key, h_value|
      @config.setProperty([HOSTS, host_alias, h_key], h_value)
    }
    @config.setProperty([HOSTS, host_alias, FIXED_PROPERTY_STRINGS], 
      @config.getPropertyOr([HOSTS, host_alias, FIXED_PROPERTY_STRINGS], []) + 
      Configurator.instance.fixed_properties)
    
    is_valid?()
  end
  
  def output_usage()
    host_alias = @config.getPropertyOr(HOSTS, {}).keys.at(0)
    
    puts "Usage: update [general-options] [target-options] [install-options]"
    output_general_usage()
    
    Configurator.instance.write_divider()
    puts "Target options:"
    output_usage_line("--host", "Host to connect to configure the service", @target_host)
    output_usage_line("--user", "User to connect to the host as", @target_user)
    output_usage_line("--release-directory", "The release directory that holds the Tungsten runtime files", @target_home_directory)
    
    Configurator.instance.write_divider(Logger::ERROR)
    puts "Install options:"
    
    each_host_prompt{
      |prompt|
      prompt.set_member(host_alias || DEFAULTS)
      prompt.output_usage()
    }
  end
end

module NotTungstenUpdatePrompt
  def enabled_for_command_line?
    unless Configurator.instance.package.is_a?(UpdatePackage)
      super() && true
    else
      false
    end
  end
end