module ConfigureDeploymentCore
  include ConfigureMessages
  include EventHandler
  
  def initialize(config, modules)
    super()
    @config = config
    @services = []
    @deployment_methods = []
    
    modules.each{
      |module_def|
      self.extend(module_def)
      begin
        begin
          module_def.get_deployment_methods.each{
            |dm|
            @deployment_methods << dm
          }
        rescue NoMethodError
        end
        
        begin
          module_def.bind_listeners(self, @config)
        rescue NoMethodError
        end
      rescue => e
        exception(e)
      end
    }
    @deployment_methods.sort!{|a,b| a.weight <=> b.weight}
  end
  
  def deploy
    begin
      trigger_event(:before_deployment)
      
      @deployment_methods.each{
        |deployment_method|
        trigger_event(:before_deployment_step_method, deployment_method.method_name)
        self.send(alter_deployment_method_name(deployment_method.method_name))
        trigger_event(:after_deployment_step_method, deployment_method.method_name)
      }
      
      trigger_event(:after_deployment)
    rescue => e
      pp e
      error(e.to_s() + ":\n" + e.backtrace.join("\n"))
    end
    
    result = RemoteResult.new()
    result.errors = @errors
    return result
  end
  
  def get_deployment_basedir
    @config.getProperty(CURRENT_RELEASE_DIRECTORY)
  end
  
  def get_deployment_config_file
    if @config.getProperty(HOME_DIRECTORY) == get_deployment_basedir()
      return "#{@config.getProperty(HOME_DIRECTORY)}/#{Configurator::HOST_CONFIG}"
    else
      mkdir_if_absent("#{@config.getProperty(HOME_DIRECTORY)}/configs")
      return "#{@config.getProperty(HOME_DIRECTORY)}/configs/#{Configurator::HOST_CONFIG}"
    end
  end
  
  def alter_deployment_method_name(method_name)
    method_name
  end
  
  # Create a directory if it is absent. 
  def mkdir_if_absent(dirname)
    if dirname == nil
      return
    end
    
    if File.exists?(dirname)
      if File.directory?(dirname)
        debug("Found directory, no need to create: #{dirname}")
      else
        raise "Directory already exists as a file: #{dirname}"
      end
    else
      debug("Creating missing directory: #{dirname}")
      cmd_result("mkdir -p #{dirname}")
    end
  end
  
  def get_root_prefix()
    prefix = @config.getProperty(ROOT_PREFIX)
    if prefix == "true" or prefix == "sudo"
      return "sudo"
    else
      return ""
    end
  end
  
  def svc_is_running?(cmd)
    begin
      cmd_result("#{cmd} status")
      return true
    rescue CommandError => ce
      return false
    end
    
    return false
  end
  
  # Update the RUN_AS_USER in a service script.
  def set_run_as_user(script)
    transformer = Transformer.new(script, script, nil)
    transformer.set_fixed_properties(@config.getProperty(get_host_key(FIXED_PROPERTY_STRINGS)))

    # Have to be careful to set first RUN_AS_USER= only or it
    # corrupts the start script.
    already_set = false
    transformer.transform { |line|
      if line =~ /RUN_AS_USER=/ && ! already_set then
        already_set = true
        "RUN_AS_USER=" + @config.getProperty(USERID)
      else
        line
      end
    }
  end
  
  def get_svc_command(boot_script)
    prefix = get_root_prefix()
    if prefix == ""
      return boot_script
    else
      return prefix + " " + boot_script
    end
  end
  
  # Generate a cluster service properties file for a system service.
  def write_svc_properties(name, boot_script)
    cluster = @config.getProperty(CLUSTERNAME)
    props_name = name + ".properties"
    svc_properties_dir = "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + cluster + "/service/"
    svc_properties = svc_properties_dir + "/" + props_name
    svc_command = get_svc_command(boot_script)

    # Ensure services properties directory exists.
    mkdir_if_absent(svc_properties_dir)

    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts "# #{props_name}"
    out.puts "name=#{name}"
    out.puts "command.start=#{svc_command} start"
    out.puts "command.stop=#{svc_command} stop"
    out.puts "command.restart=#{svc_command} restart"
    out.puts "command.status=#{svc_command} status"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + svc_properties
  end
  
  # Add an OS service that needs to be started and/or deployed.
  def add_service(start_script)
    @services.insert(-1, start_script)
  end
  
  def get_host_key(key)
    [HOSTS, @config.getProperty(DEPLOYMENT_HOST), key]
  end
  
  # Find out the full executable path or return nil
  # if this is not executable. 
  def which(cmd)
    if ! cmd
      nil
    else 
      path = cmd_result("which #{cmd}")
      path.chomp!
      if File.executable?(path)
        path
      else
        nil
      end
    end
  end
  
  def get_message_hostname
    @config.getProperty(DEPLOYMENT_HOST)
  end
  
  def is_connector?
    false
  end
  
  def is_replicator?
    false
  end
  
  def is_master?
    false
  end
  
  def transform_values(matches)
	  case matches.at(0)
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])), method(:transform_values))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      }, method(:transform_values))
    end
    
    return v
	end
end

class ConfigureDeploymentMethod
  attr_reader :method_name, :weight
  def initialize(method_name, weight = 0)
    @method_name=method_name
    @weight=weight
  end
end