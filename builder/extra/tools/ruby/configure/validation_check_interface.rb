module ValidationCheckInterface
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
    
    super()
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
  
  def get_userid
    @config.getProperty(USERID)
  end
  
  def get_hostname
    @config.getProperty(HOST)
  end
  
  def ssh_result(command, ignore_fail = false, host = nil, user = nil)
    if (user == nil)
      user = get_userid()
    end
    if (host == nil)
      host = get_hostname()
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
  
  def build_error_object(message)
    get_error_object_class().new(message, get_message_hostname(), self)
  end
  
  def get_error_object_class
    ValidationError
  end
end