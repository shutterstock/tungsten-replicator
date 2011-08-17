class ConfigureDeploymentHandler
  include ConfigureMessages
  
  def initialize(deployment_method)
    super()
    @deployment_method = deployment_method
    @config = Properties.new()
  end
  
  def prepare(configs)
    reset_errors()
    configs.each{
      |config|
      
      prepare_config(config)
    }
    
    is_valid?()
  end
  
  def prepare_config(config)
    @config.props = config.props
    
    unless Configurator.instance.is_localhost?(@config.getProperty(HOST))
      validation_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}/"
      
      # Transfer validation code
      debug("Transfer configuration code to #{@config.getProperty(HOST)}")
      cmd_result("rsync -Caze ssh --delete #{Configurator.instance.get_base_path()}/ #{@config.getProperty(USERID)}@#{@config.getProperty(HOST)}:#{validation_temp_directory}")

      # Transfer the MySQL/J file if it is being used
      if @config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH) != nil
        debug("Transfer Connector/J to #{@config.getProperty(HOST)}")
        cmd_result("scp #{@config.getProperty(GLOBAL_REPL_MYSQL_CONNECTOR_PATH)} #{@config.getProperty(USERID)}@#{@config.getProperty(HOST)}:#{validation_temp_directory}")
      end

      debug("Transfer host configuration file to #{@config.getProperty(HOST)}")
      config_tempfile = Tempfile.new("tcfg")
      config_tempfile.close()
      config.store(config_tempfile.path())
      cmd_result("scp #{config_tempfile.path()} #{@config.getProperty(USERID)}@#{@config.getProperty(HOST)}:#{validation_temp_directory}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}")
      File.unlink(config_tempfile.path())
    end
  end
  
  def deploy(configs)
    reset_errors()
    configs.each{
      |config|
      
      deploy_config(config)
    }
    
    is_valid?()
  end
  
  def deploy_config(config)
    @config.props = config.props
    
    if Configurator.instance.is_localhost?(@config.getProperty(HOST))
      Configurator.instance.write ""
      Configurator.instance.write_header "Local deploy #{@config.getProperty(HOME_DIRECTORY)}"
      
      result = @deployment_method.deploy_config(config)
    else
      extra_options = ["--package #{Configurator.instance.package.class.name}"]
      if Configurator.instance.enable_log_level?(Logger::DEBUG)
        extra_options << "-v"
      end
      unless Configurator.instance.enable_log_level?(Logger::INFO)
        extra_options << "-q"
      end
      
      Configurator.instance.write ""
      Configurator.instance.write_header "Remote deploy #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
      
      deployment_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}"
      command = "cd #{deployment_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/deploy.rb -b -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} #{extra_options.join(' ')}"
      debug(command)
      
      if Configurator.instance.use_streaming_ssh()
        result_dump = ""
        ssh = Net::SSH.start(@config.getProperty(HOST), @config.getProperty(USERID))
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
      
      begin
        result = Marshal.load(result_dump)
        
        @errors = @errors + result.errors
        result.output()
      rescue TypeError => te
        raise "Cannot read the deployment result: #{result_dump}"
      rescue ArgumentError => ae
        raise "Unable to load the deployment result: #{result_dump}"
      end
    end
  end
  
  def cleanup(configs)
    reset_errors()
    configs.each{
      |config|
      
      @config.props = config.props
      begin
        unless Configurator.instance.is_localhost?(@config.getProperty(HOST))
          ssh_result("rm -rf #{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}", true)
        end
      rescue
      end
    }
    
    is_valid?()
  end
  
  def get_message_hostname
    @config.getProperty(HOST)
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
    result = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; export LANG=en_US; #{command}\" 2>&1`.chomp
    rc = $?
    
    if rc != 0 && ! ignore_fail
      raise RemoteCommandError.new(user, host, command, rc, result)
    else
      debug("RC: #{rc}, Result: #{result}")
    end
    
    return result
  end
end