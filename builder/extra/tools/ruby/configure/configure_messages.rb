module ConfigureMessages
  attr_reader :errors
  
  def initialize
    reset_errors()
  end
  
  def reset_errors
    @errors = []
  end
  
  def is_valid?()
    (@errors.length() == 0)
  end
  
  def info(message)
    Configurator.instance.info(message, get_message_hostname())
  end
  
  def warning(message)
    Configurator.instance.warning(message, get_message_hostname())
  end
  
  def error(message, e = nil)
    Configurator.instance.error(message, get_message_hostname())
    
    unless @errors
      @errors = []
    end
    
    store_error_object(message, e)
  end

  def store_error_object(message, e = nil)
    if e == nil
      e = build_error_object(message)
    end
    
    @errors.push(e)
  end
  
  def build_error_object(message)
    get_error_object_class().new(message, get_message_hostname())
  end
  
  def get_error_object_class
    RemoteError
  end
  
  def debug(message)
    Configurator.instance.debug(message, get_message_hostname())
  end
  
  def get_message_hostname
    nil
  end
end

class RemoteError < StandardError
  attr_reader :message, :host

  def initialize(message, host = nil)
    if host == nil
      host = `hostname`.chomp()
    end
    
    @message=message
    @host=host
  end
end

class RemoteResult
  attr_accessor :messages, :errors
  
  def initialize
    @messages = []
    @errors = []
  end
  
  def output
    @messages.each{
      |message|
      puts message
    }
  end
end