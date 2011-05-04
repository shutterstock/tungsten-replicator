class ConfigureDeploymentHandler
  include ConfigureMessages
  
  def initialize(deployment_method)
    super()
    @deployment_method = deployment_method
    @config = Properties.new()
  end
  
  def run(configs)
    reset_errors()
    configs.each{
      |config|
      result = run_config(config)
      @errors = @errors + result.errors
      
      result.output()
    }
    
    is_valid?()
  end
  
  def run_config(config)
    @config.props = config.props.dup().merge(config.getPropertyOr([HOSTS, config.getProperty(DEPLOYMENT_HOST)], {}))
    
    if @config.getProperty(HOST) == Configurator.instance.hostname()
      Configurator.instance.write ""
      Configurator.instance.write_header "Local deploy #{@config.getProperty(HOME_DIRECTORY)}"
      
      result = @deployment_method.deploy_config(@config)
    else
      extra_options = ""
      if Configurator.instance.enable_log_level(Logger::DEBUG)
        extra_options = "-v"
      end
      unless Configurator.instance.enable_log_level(Logger::INFO)
        extra_options = "-q"
      end
      
      Configurator.instance.write ""
      Configurator.instance.write_header "Remote deploy #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
      
      deployment_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator::TEMP_DEPLOY_DIRECTORY}/#{Configurator.instance.get_basename()}"
      command = "cd #{deployment_temp_directory}; ruby -I#{Configurator.instance.get_ruby_prefix()} -I#{Configurator.instance.get_ruby_prefix()}/lib #{Configurator.instance.get_ruby_prefix()}/deploy.rb -c #{Configurator::TEMP_DEPLOY_HOST_CONFIG} #{extra_options}"
      
      if Configurator.instance.use_streaming_ssh()
        Configurator.instance.debug("Execute `#{command}` on #{@config.getProperty(HOST)}")
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
        user = @config.getProperty(USERID)
        host = @config.getProperty(HOST)
        Configurator.instance.debug("Execute `#{command}` on #{host}")
        result_dump = `ssh #{user}@#{host} -o \"PreferredAuthentications publickey\" -o \"IdentitiesOnly yes\" -o \"StrictHostKeyChecking no\" \". /etc/profile; #{command}\" 2>/dev/null`.chomp
        rc = $?

        if rc != 0
          raise "Failed: #{command}, RC: #{rc}, Result: #{result_dump}"
        else
          Configurator.instance.debug("RC: #{rc}, Result: #{result_dump}")
        end
      end
      
      begin
        result = Marshal.load(result_dump)
      rescue ArgumentError => ae
        raise "Unable to load deployment result: #{result_dump}"
      end
    end
    
    result
  end
  
  def get_message_hostname
    @config.getProperty(HOST)
  end
end