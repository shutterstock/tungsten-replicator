class ConfigurePackage
  include ConfigureMessages
  
  def initialize(config)
    @config = config
  end
  
  def store_config_file?
    false
  end
  
  def read_config_file?
    true
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
    output_usage_line("-p, --preview", "Displays the help message and preview the effect of the command line options")
    output_usage_line("-q, --quiet", "Only display error messages")
    output_usage_line("-v, --verbose", "Display all messages")
    output_usage_line("--net-ssh-option=key=value", "Set the Net::SSH option for remote system calls", nil, nil, "Valid options can be found at http://net-ssh.github.com/ssh/v2/api/classes/Net/SSH.html#M000002")
    
    if Configurator.instance.advanced_mode?()
      output_usage_line("--config-file-help", "Display help information for content of the config file")
      output_usage_line("--template-file-help", "Display the keys that may be used in configuration template files")
      output_usage_line("--no-validation", "Skip all validation checks")
      output_usage_line("--validate-only", "Skip all deployment steps")
      output_usage_line("--skip-validation-check String", "Do not run the specified validation check.  Validation checks are identified by the string included in their error they output.")
      output_usage_line("--property=key=value")
      output_usage_line("--property=key+=value")
      output_usage_line("--property=key~=/match/replace/", "Modify the value for key in any file that the configure script touches", "", nil, 
        "key=value\t\t\tSet key to value without evaluating template values or other rules<br>
        key+=value\t\tEvaluate template values and then append value to the end of the line<br>
        key~=/match/replace/\tEvaluate template values then excecute the specified Ruby regex with sub<br>
        <br>
        --property=replicator.key~=/(.*)/somevalue,\\1/ will prepend 'somevalue' before the template value for 'replicator.key'<br><br>
        Reference the String::sub! function for details on how to build the match or replace parameters.<br>http://ruby-doc.org/core/classes/String.html#M001185")
    end
  end
  
  def each_host_prompt(&block)
    ch = ClusterHosts.new()
    ch.set_config(@config)
    
    ch.each_prompt{
      |prompt|
      
      if prompt.enabled_for_command_line?()
        begin
          block.call(prompt)
        rescue => e
          error(e.message)
        end
      end
    }
  end
  
  def each_service_prompt(&block)
    ch = ReplicationServices.new()
    ch.set_config(@config)
    
    ch.each_prompt{
      |prompt|
      
      if prompt.enabled_for_command_line?()
        begin
          block.call(prompt)
        rescue => e
          error(e.message)
        end
      end
    }
  end
  
  def each_datasource_prompt(&block)
    ch = Datasources.new()
    ch.set_config(@config)
    
    ch.each_prompt{
      |prompt|
      
      if prompt.enabled_for_command_line?()
        begin
          block.call(prompt)
        rescue => e
          error(e.message)
        end
      end
    }
  end
  
  def load_target_config(arguments)
    deployment_host = @config.getNestedProperty([DEPLOYMENT_HOST])
    if deployment_host.to_s == ""
      deployment_host = DEFAULTS
    end
    
    if deployment_host == DEFAULTS
      @target_host = Configurator.instance.hostname
    else
      @target_host = @config.getProperty([HOSTS, deployment_host, HOST])
    end
    @target_user = @config.getProperty([HOSTS, deployment_host, USERID])
    @target_home_directory = @config.getProperty([HOSTS, deployment_host, CURRENT_RELEASE_DIRECTORY])
    @load_remote_config = false
    
    opts=OptionParser.new
    opts.on("--host String")    { |val| 
      @load_remote_config = true
      @target_host = val }
    opts.on("--user String")    { |val| 
      @target_user = val }
    opts.on("--release-directory String")  { |val| 
      @load_remote_config = true
      @target_home_directory = val }
    
    arguments = Configurator.instance.run_option_parser(opts, arguments)

    if @load_remote_config == true
      @target_validated = false
      
      if ssh_result("if [ -d #{@target_home_directory} ]; then if [ -x #{@target_home_directory}/tools/configure ]; then echo 0; else echo 1; fi else echo 1; fi", @target_host, @target_user) == "0"
        @target_validated = true
      else
        unless @target_home_directory =~ /tungsten[\/]?$/
          @target_home_directory = @target_home_directory + "/tungsten"
          if ssh_result("if [ -d #{@target_home_directory} ]; then if [ -x #{@target_home_directory}/tools/configure ]; then echo 0; else echo 1; fi else echo 1; fi", @target_host, @target_user) == "0"
            @target_validated = true
          end
        end
      end
      
      if @target_validated == false
        unless ssh_result("if [ -d #{@target_home_directory} ]; then echo 0; else echo 1; fi", @target_host, @target_user) == "0"
          raise "Unable to find a Tungsten directory at #{@target_home_directory}"
        else
          raise "Unable to find #{@target_home_directory}/tools/configure.  Make sure that you have installed version 2.0.4 or greater"
        end
      end
      
      info "Load the current config from #{@target_user}@#{@target_host}:#{@target_home_directory}"
      
      begin
        command = "#{@target_home_directory}/tools/configure --output-config"    
        config_output = ssh_result(command, @target_host, @target_user)
        parsed_contents = JSON.parse(config_output)
        unless parsed_contents.instance_of?(Hash)
          raise "invalid object"
        end
        @config.props = parsed_contents.dup
      rescue => e
        exception(e)
        raise "Unable to load the current config from #{@target_user}@#{@target_host}:#{@target_home_directory}"
      end
    end
    
    arguments
  end
end