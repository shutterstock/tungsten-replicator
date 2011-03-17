class ConfigureDeploymentHandler
  include ConfigureMessages
  
  def initialize(config)
    super()
    @config = config
  end
  
  def run(configs)
    reset_errors()
    configs.each{
      |config|
      @config.props = config.props
        
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
        
        Configurator.instance.write ""
        Configurator.instance.write_header "Local deploy #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}"
        
        command = "cd #{Configurator.instance.get_base_path()}; ruby -I#{Configurator.instance.get_ruby_prefix()} #{Configurator.instance.get_ruby_prefix()}/deploy.rb -c #{config_tempfile.path()} #{extra_options} --stream"
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
        Configurator.instance.write ""
        Configurator.instance.write_header "Remote deploy #{@config.getProperty(GLOBAL_HOST)}:#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}"
        
        deployment_temp_directory = "#{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{Configurator::TEMP_DEPLOY_DIRECTORY}/#{Configurator.instance.get_basename()}"
        command = "cd #{deployment_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} #{Configurator.instance.get_ruby_prefix()}/deploy.rb -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} #{extra_options}"
        
        if Configurator.instance.use_streaming_ssh()
          Configurator.instance.debug("Execute `#{command}` on #{@config.getProperty(GLOBAL_HOST)}")
          result_dump = ""
          ssh = Net::SSH.start(@config.getProperty(GLOBAL_HOST), @config.getProperty(GLOBAL_USERID))
          ssh.exec!(". /etc/profile; #{command} --stream") do
            |ch, stream, data|
            unless data =~ /RemoteResult/ || result_dump != ""
              puts data
            else
              result_dump = data
            end
          end
          ssh.close()
        else
          result_dump = ssh_result(command)
        end
      end
      
      begin
        result = Marshal.load(result_dump)
      rescue ArgumentError => ae
        raise "Unable to load deployment result: #{result_dump}"
      end
      
      @errors = @errors + result.errors
      
      result.output()
    }
    
    is_valid?()
  end
  
  # Handle the remote side of the run function
  def deploy_config
    # Get the object for the specific deployment type in the config
    config_deployment = Configurator.instance.get_deployment()
    # Get an object that represents the deployment steps required by the config
    obj = config_deployment.get_deployment_object()
    # Execute each of the deployment steps
    obj.deploy()
  end
  
  def cmd_result(command, ignore_fail = false)
    debug("Execute `#{command}`")
    result = `#{command} 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise "Failed: #{command}, RC: #{rc}, Result: #{result}"
    else
      Configurator.instance.debug("RC: #{rc}, Result: #{result}")
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
    
    Configurator.instance.debug("Execute `#{command}` on #{host}")
    result = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise "Failed: #{command}, RC: #{rc}, Result: #{result}"
    else
      Configurator.instance.debug("RC: #{rc}, Result: #{result}")
    end
    
    return result
  end
  
  def get_message_hostname
    @config.getProperty(GLOBAL_HOST)
  end
end