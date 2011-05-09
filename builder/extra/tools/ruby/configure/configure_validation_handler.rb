class ConfigureValidationHandler
  include ConfigureMessages
  attr_reader :deployment_checks, :local_checks
  
  def initialize()
    super()
    @deployment_checks = []
    @local_checks = []
    @config = Properties.new()
    initialize_validation_checks()
  end
  
  # Tell each ConfigureModule to register their prompts
  def initialize_validation_checks
    @local_checks = []
    @deployment_checks = []
    
    register_checks(Configurator.instance.package.get_validation_checks())
  end
  
  def register_checks(check_objs)
    check_objs.each{|check_obj| register_check(check_obj)}
  end
  
  # Validate the prompt object and add it to the queue
  def register_check(check_obj)    
    unless check_obj.is_a?(ValidationCheckInterface)
      raise "Attempt to register invalid check #{check_obj.class} failed " +
        "because it does not extend ValidationCheckInterface"
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
  
  # The preliminary checks look for ssh access, ruby and java
  def prevalidate(configs)
    reset_errors()
    
    configs.each {
      |config|
      
      prevalidate_config(config)
    }
    
    is_valid?()
  end
  
  def prevalidate_config(config)
    @config.props = config.props.dup().merge(config.getPropertyOr([HOSTS, config.getProperty(DEPLOYMENT_HOST)], {}))

    Configurator.instance.write ""
    Configurator.instance.write_header "Preliminary checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"

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
        @errors.push(ValidationError.new(e.to_s(), @config.getProperty(HOST), check))
      end
    }
  end
  
  # These checks are more in-depth
  def validate(configs)
    reset_errors()
    
    configs.each{
      |config|
      
      @config.props = config.props.dup().merge(config.getPropertyOr([HOSTS, config.getProperty(DEPLOYMENT_HOST)], {}))

      Configurator.instance.write ""
      Configurator.instance.write_header "Validation checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"

      begin
        if Configurator.instance.is_localhost?(@config.getProperty(HOST))
          debug("Start:  Local validation checks for #{@config.getProperty(HOME_DIRECTORY)}")
          validate_config(@config)
          debug("Finish: Local validation checks for #{@config.getProperty(HOME_DIRECTORY)}")
        else
          # Invoke ValidationChecks on the remote server
          extra_options = ""
          if Configurator.instance.enable_log_level(Logger::DEBUG)
            extra_options = "-v"
          end
          unless Configurator.instance.enable_log_level(Logger::INFO)
            extra_options = "-q"
          end

          validation_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator::TEMP_DEPLOY_DIRECTORY}/#{Configurator.instance.get_basename()}/"

          debug("Remote validation checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}")
          command = "cd #{validation_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/validate.rb -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} #{extra_options}"

          if Configurator.instance.use_streaming_ssh()
            result_dump = ""
            ssh = Net::SSH.start(@config.getProperty(HOST), @config.getProperty(USERID))
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
            user = @config.getProperty(USERID)
            host = @config.getProperty(HOST)
            Configurator.instance.debug("Execute `#{command}` on #{host}")
            result_dump = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>/dev/null`.chomp
            rc = $?

            if rc != 0
              raise RemoteCommandError(user, host, command, rc, result_dump)
            else
              Configurator.instance.debug("RC: #{rc}, Result: #{result_dump}")
            end
          end

          begin
            result = Marshal.load(result_dump)

            @errors = @errors + result.errors
            result.output()

            Configurator.instance.write "Finish: Remote validation checks for #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}", Logger::DEBUG
          rescue ArgumentError => ae
            raise "Unable to load validation result: #{result_dump}"
          end
        end
      rescue => e
        error(e.to_s())
      end
    }
    
    is_valid?()
  end
  
  # Handle the remote side of the validate function
  def validate_config(config)
    @config.props = config.props
    
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
      @errors.push(RemoteError.new(e.to_s(), @config.getProperty(HOST)))
    end
    
    # Prepare the return object
    result = RemoteResult.new
    result.errors = @errors
    result
  end
  
  def ssh_result(command, ignore_fail = false, host = nil, user = nil)
    if (user == nil)
      user = @config.getProperty(USERID)
    end
    if (host == nil)
      host = @config.getProperty(HOST)
    end
    
    if Configurator.instance.is_localhost?(host) && user == Configurator.instance.whoami()
      return cmd_result(command, ignore_fail)
    end
    
    debug("Execute `#{command}` on #{host}")
    result = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise RemoteCommandError.new(user, host, command, rc, result)
    else
      debug("RC: #{rc}, Result: #{result}")
    end
    
    return result
  end
  
  def get_message_hostname
    @config.getProperty(HOST)
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