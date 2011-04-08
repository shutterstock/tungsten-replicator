module ConfigureDeploymentCore
  include ConfigureMessages
  
  def initialize(config)
    super()
    @config = config
    @services = []
    @deployment_methods = []
  end
  
  def set_deployment_methods(deployment_methods)
    @deployment_methods = deployment_methods.sort{|a,b| a.weight <=> b.weight}
  end
  
  def deploy
    begin
      @deployment_methods.each{
        |deployment_method|
        self.send(deployment_method.method_name)
      }
    rescue Exception => e
      error("#{e.to_s()}:#{e.backtrace.join("\n")}")
    end
    
    result = RemoteResult.new()
    result.errors = @errors
    return result
  end
  
  def fill_ports_near_hosts(host_list, port_to_add)
    host_list = host_list.split(",")
    initial_hosts = nil
    host_list.each { |host|
      host_addr = host.strip + "[" + port_to_add + "]"
      if initial_hosts
        initial_hosts = initial_hosts + "," + host_addr
      else
        initial_hosts = host_addr
      end
    }
    return initial_hosts
  end
  
  def get_deployment_basedir
    @config.getProperty(GLOBAL_BASEDIR)
  end
  
  # Create a directory if it is absent. 
  def mkdir_if_absent(dirname)
    if dirname == nil
      return
    end
    
    if File.exists?(dirname)
      debug("Found directory, no need to create: #{dirname}")
    else
      debug("Creating missing directory: #{dirname}")
      Dir.mkdir(dirname)
    end
  end
  
  def get_root_prefix()
    prefix = @config.getProperty(GLOBAL_ROOT_PREFIX)
    if prefix == "true" or prefix == "sudo"
      return "sudo"
    else
      return ""
    end
  end
  
  # Update the RUN_AS_USER in a service script.
  def set_run_as_user(script)
    transformer = Transformer.new(script, script, nil)

    # Have to be careful to set first RUN_AS_USER= only or it
    # corrupts the start script.
    already_set = false
    transformer.transform { |line|
      if line =~ /RUN_AS_USER=/ && ! already_set then
        already_set = true
        "RUN_AS_USER=" + @config.getProperty(GLOBAL_USERID)
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
    cluster = @config.getProperty(GLOBAL_DSNAME)
    props_name = name + ".properties"
    svc_properties_dir = "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + cluster + "/service/"
    svc_properties = svc_properties_dir + "/" + props_name
    svc_command = get_svc_command(boot_script)

    # Ensure services properties directory exists.
    cmd_result("mkdir -p #{svc_properties_dir}")

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
    @config.getProperty(GLOBAL_HOST)
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
end

class ConfigureDeploymentMethod
  attr_reader :method_name, :weight
  def initialize(method_name, weight = 0)
    @method_name=method_name
    @weight=weight
  end
end