class ConfigureValidationCheck
  include ConfigureMessages
  attr_reader :title, :description, :properties, :messages, :fatal_on_error, :support_remote_fix
  def initialize
    @title = nil
    @description = ""
    @properties = []
    @config = nil
    @fatal_on_error = false
    @help = []
    @support_remote_fix = false
    reset_errors()
    
    set_vars()
    
    if @title == nil
      raise "'title' has not been set"
    end
  end
  
  def run
    reset_errors()
    unless enabled?()
      return
    end
    
    begin
      info("Start: #{@title}")
      validate()
    rescue Exception => e
      error(e.to_s())
    ensure
      info("Finish: #{@title}")
    end
  end
  
  def set_vars
    raise "The 'set_vars' function should be overwritten"
  end
  
  def validate
    class_name = self.class().name()
    error("The 'validate' function for #{class_name} should be overwritten")
  end
  
  def enabled?
    true
  end
  
  def fatal_on_error?
    @fatal_on_error
  end
  
  def set_config(config)
    @config = config
  end
  
  def is_initialized?
    (@title != nil)
  end
  
  def help(message)
    @help << message
  end
  
  def get_help
    if @help.empty?()
      nil
    else
      @help
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
  
  def build_error_object(message)
    get_error_object_class().new(message, get_message_hostname(), self)
  end
  
  def get_error_object_class
    ValidationError
  end
  
  def is_connector?
    (@config.getPropertyOr(CONN_HOSTS, "").split(",").include?(@config.getProperty(GLOBAL_HOST)))
  end
  
  def is_replicator?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      service_hosts = service_properties[REPL_HOSTS].split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        return true
      end
    }
    
    false
  end
  
  def is_master?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      if service_properties[REPL_MASTERHOST] == @config.getProperty(GLOBAL_HOST)
        return true
      end
    }
    
    false
  end
end

module LocalValidationCheck
end