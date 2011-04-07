class ConfigureValidationHandler
  include ConfigureMessages
  attr_reader :deployment_checks, :local_checks
  
  def initialize(config)
    super()
    @deployment_checks = []
    @local_checks = []
    @config = config
    initialize_validation_checks()
  end
  
  # Tell each ConfigureModule to register their prompts
  def initialize_validation_checks
    @local_checks = []
    @deployment_checks = []
    Configurator.instance.modules.each{
      |configure_module| 
      configure_module.register_validation_checks(self)
    }
  end
  
  def register_checks(check_objs)
    check_objs.each{|check_obj| register_check(check_obj)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_check(check_obj)    
    unless check_obj.is_a?(ConfigureValidationCheck)
      raise "Attempt to register invalid check #{class_name} failed " +
        "because it does not extend ConfigureValidationCheck"
    end
    
    check_obj.set_config(@config)
    unless check_obj.is_initialized?()
      raise "#{class_name} cannot be used because it has not been properly initialized"
    end
    
    if check_obj.is_a?(LocalValidationCheck)
      @local_checks.push(check_obj)
    else
      @deployment_checks.push(check_obj)
    end
  end
  
  def run(configs)
    reset_errors()
    
    configs.each{
      |config|
      @config.props = config.props
      preliminary_validation()
    }
    
    unless is_valid?()
      return
    end
    
    configs.each{
      |config|
      @config.props = config.props
      validate()
    }
    
    is_valid?()
  end
  
  # The preliminary checks look for ssh access, ruby and java
  def preliminary_validation
    Configurator.instance.write ""
    Configurator.instance.write_header "Preliminary checks for #{@config.getProperty(GLOBAL_HOST)}:#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}"
    
    # Preliminary checks to ensure connectivity and transfer validation code
    @local_checks.each{
      |check|
      begin
        check.run()
        @errors = @errors + check.errors
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      rescue ValidationError => ve
        Configurator.instance.exception(ve)
        @errors.push(ve)
      rescue Exception => e
        Configurator.instance.exception(e)
        @errors.push(ValidationError.new(e.to_s(), @config.getProperty(GLOBAL_HOST), check))
      end
    }
  end
  
  # These checks are more in-depth
  def validate
    Configurator.instance.write ""
    Configurator.instance.write_header "Validation checks for #{@config.getProperty(GLOBAL_HOST)}:#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}"
    
    begin
      # Invoke ValidationChecks on the remote server
      extra_options = ""
      if Configurator.instance.enable_log_level(Logger::DEBUG)
        extra_options = "-v"
      end
      unless Configurator.instance.enable_log_level(Logger::INFO)
        extra_options = "-q"
      end
      
      if @config.getProperty(GLOBAL_HOST) == Configurator.instance.hostname()
        config_tempfile = Tempfile.new("tcfg")
        config_tempfile.close()
        @config.store(config_tempfile.path())

        debug("Local validation checks for #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}")
        command = "cd #{Configurator.instance.get_base_path()}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/validate.rb -c #{config_tempfile.path()} #{extra_options} --stream"
        result_dump = ""

        val_proc = IO.popen(command)
        while (data = val_proc.gets())
          unless data =~ /RemoteResult/ || result_dump != ""
        		puts data
          else
            result_dump += data
          end
        end
        val_proc.close()
      else
        cluster_config = Properties.new()
        cluster_config.load(Configurator.instance.get_config_filename())
        
        # Transfer validation code
        validation_temp_directory = "#{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{Configurator::TEMP_DEPLOY_DIRECTORY}/#{Configurator.instance.get_basename()}/"
        debug("Transfer configuration code to #{@config.getProperty(GLOBAL_HOST)}")
        ssh_result("mkdir -p #{validation_temp_directory}")
        cmd_result("rsync -Caze ssh --delete #{Configurator.instance.get_base_path()}/ #{@config.getProperty(GLOBAL_USERID)}@#{@config.getProperty(GLOBAL_HOST)}:#{validation_temp_directory}")

        if @config.getProperty(REPL_MYSQL_CONNECTOR_PATH) != nil
          debug("Transfer Connector/J to #{@config.getProperty(GLOBAL_HOST)}")
          cmd_result("scp #{@config.getProperty(REPL_MYSQL_CONNECTOR_PATH)} #{@config.getProperty(GLOBAL_USERID)}@#{@config.getProperty(GLOBAL_HOST)}:#{validation_temp_directory}")
          @config.setProperty(REPL_MYSQL_CONNECTOR_PATH, "#{validation_temp_directory}/#{File.basename(@config.getProperty(REPL_MYSQL_CONNECTOR_PATH))}")
          cluster_config.setProperty(REPL_MYSQL_CONNECTOR_PATH, "#{validation_temp_directory}/#{File.basename(@config.getProperty(REPL_MYSQL_CONNECTOR_PATH))}")
        end
        
        debug("Transfer cluster configuration file to #{@config.getProperty(GLOBAL_HOST)}")
        cluster_config_tempfile = Tempfile.new("tcfg")
        cluster_config_tempfile.close()
        cluster_config.store(cluster_config_tempfile.path())
        cmd_result("scp #{cluster_config_tempfile.path()} #{@config.getProperty(GLOBAL_USERID)}@#{@config.getProperty(GLOBAL_HOST)}:#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_CLUSTER_CONFIG}")
        File.unlink(cluster_config_tempfile.path())
        
        debug("Transfer host configuration file to #{@config.getProperty(GLOBAL_HOST)}")
        config_tempfile = Tempfile.new("tcfg")
        config_tempfile.close()
        @config.store(config_tempfile.path())
        cmd_result("scp #{config_tempfile.path()} #{@config.getProperty(GLOBAL_USERID)}@#{@config.getProperty(GLOBAL_HOST)}:#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}")
        File.unlink(config_tempfile.path())
        
        debug("Remote validation checks for #{@config.getProperty(GLOBAL_HOST)}:#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}")
        command = "cd #{validation_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/validate.rb -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} #{extra_options}"
        
        if Configurator.instance.use_streaming_ssh()
          result_dump = ""
          ssh = Net::SSH.start(@config.getProperty(GLOBAL_HOST), @config.getProperty(GLOBAL_USERID))
          ssh.exec!(". /etc/profile; #{command} --stream") do
            |ch, stream, data|
            unless data =~ /RemoteResult/ || result_dump != ""
              puts data
            else
              result_dump += data
            end
          end
          ssh.close()
        else
          user = @config.getProperty(GLOBAL_USERID)
          host = @config.getProperty(GLOBAL_HOST)
          Configurator.instance.debug("Execute `#{command}` on #{host}")
          result_dump = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>/dev/null`.chomp
          rc = $?

          if rc != 0
            raise "Failed: #{command}, RC: #{rc}, Result: #{result_dump}"
          else
            Configurator.instance.debug("RC: #{rc}, Result: #{result_dump}")
          end
        end
      end
      
      begin
        result = Marshal.load(result_dump)
      rescue ArgumentError => ae
        raise "Unable to load validation result: #{result_dump}"
      end
      @errors = @errors + result.errors
      result.messages.each{
        |message|
        puts message
      }
      
      Configurator.instance.write "Finish: Remote validation checks for #{@config.getProperty(GLOBAL_HOST)}:#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}", Logger::DEBUG
    rescue => e
      Configurator.instance.exception(e)
      @errors.push(RemoteError.new(@config.getProperty(GLOBAL_HOST), e.to_s()))
    end
    
    is_valid?()
  end
  
  # Handle the remote side of the validate function
  def validate_config
    begin
      @deployment_checks.each{
        |check|
        check.run()
        @errors = @errors + check.errors
        
        unless check.is_valid?()
          if check.fatal_on_error?()
            break
          end
        end
      }
    rescue ValidationError => ve  
      Configurator.instance.exception(ve)
      @errors.push(ve)
    rescue Exception => e  
      Configurator.instance.exception(e)
      @errors.push(RemoteError.new(e.to_s(), @config.getProperty(GLOBAL_HOST)))
    end
    
    # Prepare the return object
    result = RemoteResult.new
    result.errors = @errors
    result
  end
  
  def output_errors
    host_errors = Hash.new()
    generic_errors = []
    
    @errors.each{
      |error|
      
      if error.host == nil || !error.is_a?(ValidationError)
        generic_errors << error
      else
        unless host_errors.has_key?(error.host)
          host_errors[error.host] = []
        end
        host_errors[error.host] << error
      end
    }
    
    host_errors.each_key{
      |host|
      
      $i = 0
      
      Configurator.instance.write_header("Errors for #{host}", Logger::ERROR)
      until $i >= host_errors[host].length() do
        $host_error = host_errors[host][$i]
        $i+=1
        $next_host_error = host_errors[host][$i]
        
        Configurator.instance.error($host_error.message, host)
        
        if $next_host_error == nil || $host_error.check != $next_host_error.check
          help = $host_error.get_help()
          unless help == nil || help.empty?()
            puts help.join("\n")
          end
          
          # Disable this section for now
          if $host_error.check.support_remote_fix && Configurator.instance.is_interactive?() && false
            execute_fix = input_value("Do you want the script to automatically fix this?", "false")
          end
          
          unless help == nil || help.empty?()
            Configurator.instance.write_divider(Logger::ERROR)
          end
        end
      end
    }
    
    unless generic_errors.empty?()
      previous_help = nil
      
      Configurator.instance.write_header('Errors for the cluster', Logger::ERROR)
      generic_errors.each{
        |generic_error|
        Configurator.instance.error(generic_error.message)
      }
    end
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
  
  def get_message_hostname
    @config.getProperty(GLOBAL_HOST)
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
end

class ValidationError < RemoteError
  attr_reader :check
  
  def initialize(message, host, check)
    super(message, host)
    @check = check
  end
  
  def get_help
    @check.get_help()
  end
end