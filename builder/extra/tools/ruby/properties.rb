#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Manages store and load of installation properties. 

require 'system_require'
system_require 'date'

# Defines a properties object. 
class Properties
  attr_accessor :props
  
  # Initialize with some base values. 
  def initialize
    @props = {}
    @in_prompt_handler = {}
    @in_config_file_prompt_handler = {}
    @prompt_handler = nil
  end
  
  def initialize_copy(source)
    super(source)
    @props = Marshal::load(Marshal::dump(@props))
    @in_prompt_handler = {}
    @prompt_handler = nil
  end
  
  # Read properties from a file. 
  def load(properties_filename)
    file_contents = ""
    
    File.open(properties_filename, 'r') do |file|
      file.read.each_line do |line|
        line.strip!
        unless (line =~ /^#.*/)
          file_contents = file_contents + line
        end
      end
      
      begin
        parsed_contents = JSON.parse(file_contents)
      rescue Exception => e
      end
      
      if parsed_contents && parsed_contents.instance_of?(Hash)
        @props = parsed_contents
      else
        new_props = {}
        
        file.rewind()
        file.read.each_line do |line|
          line.strip!
          
          if (line =~ /^([\w\.]+)\[?([\w\.]+)?\]?\s*=\s*(\S.*)/)
            key = $1
            value = $3
        
            if $2
              new_props[key] = {} unless new_props[key]
              new_props[key][$2] = value
            else
              new_props[key] = value
            end
          elsif (line =~ /^([\w\.]+)\s*=/)
            key = $1
            value = ""
            new_props[key] = value
          end
        end
        
        @props = new_props
      end
      
      original_props = @props.dup
      
      getPromptHandler.update_deprecated_keys()
      
      if original_props != @props
        Configurator.instance.warning("Deprecated keys in the config file were updated")
      end
    end
  end
  
  # Read properties from a file. 
  def load_and_initialize(properties_filename, keys_module)
    load(properties_filename)
    init(keys_module)
  end
  
  def init(keys_module)
      keys_module.constants.each { |const| 
      props_key = keys_module.const_get(const.to_s)
      #puts "#{props_key}=#{@props[props_key]}"
      if (@props[props_key] == nil)
        #puts("INITIALIZE(#{props_key})")
        @props[props_key] = ""
      end
    } 
  end
  
  # Write properties to a file.  We use signal protection to avoid getting
  # interrupted half-way through. 
  def store(properties_filename, use_json = true)
    # Protect I/O with trap for Ctrl-C. 
    interrupted = false
    old_trap = trap("INT") {
      interrupted = true;
    }
    
    # Write. 
    File.open(properties_filename, 'w') do |file|
      file.printf "# Tungsten configuration properties\n"
      file.printf "# Date: %s\n", DateTime.now
      
      if use_json == false
        @props.sort.each do | key, value |
          file.printf "%s=%s\n", key, value
        end
      else
        file.printf JSON.pretty_generate(@props)
      end
    end
    
    # Check for interrupt and restore handler. 
    if (interrupted) 
      puts
      puts ("Configuration interrupted") 
      exit 1;
    else
      trap("INT", old_trap);
    end
  end
  
  # Return the size of the properties object. 
  def size()
    @props.size
  end
  
  def output()
    puts JSON.pretty_generate(@props)
  end
  
  # Fetch a nested hash value
  def getNestedProperty(attrs)
    attr_count = attrs.size
    current_val = @props
    for i in 0..(attr_count-1)
      attr_name = attrs[i]
      return current_val[attr_name] if i == (attr_count-1)
      return nil if current_val[attr_name].nil?
      current_val = current_val[attr_name]
    end
    
    return nil
  end
  
  def setNestedProperty(new_val, attrs)
    attr_count = attrs.size
    current_val = @props
    for i in 0..(attr_count-1)
      attr_name = attrs[i]
      if i == (attr_count-1)
        return setHashProperty(current_val, attr_name, new_val)
      end
      current_val[attr_name] = {} if current_val[attr_name].nil?
      current_val = current_val[attr_name]
    end
  end
  
  def setHashProperty(hash, key, value)
    if value == nil
      return (hash.delete(key))
    else
      if value.is_a?(Hash)
        hash[key] ||= {}
        value.each{|sub_key,sub_value|
          setHashProperty(hash[key], sub_key, sub_value)
        }
      else
        return (hash[key] = value)
      end
    end
  end
  
  # Get a property value. 
  def getProperty(key, allow_disabled = false)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    findProperty = lambda do |keys|
      key_string = keys.join('.')
      if @in_prompt_handler[key_string] == true
        return nil
      end

      begin
        @in_prompt_handler[key_string] = true

        value = getPromptHandler().get_property(keys, allow_disabled)

        @in_prompt_handler[key_string] = false
      rescue IgnoreError
        @in_prompt_handler[key_string] = false
      rescue => e
        @in_prompt_handler[key_string] = false
        raise e
      end

      return value
    end
    
    value = getNestedProperty(key)
    if value == nil
      value = findProperty.call(key)
    end
    
    if value == nil && key.size == 1 && (host = getNestedProperty([DEPLOYMENT_HOST]))
      value = findProperty.call([HOSTS, host, key[0]])
    end
    
    if value == nil && key.size == 1 && (svc = getNestedProperty([DEPLOYMENT_SERVICE]))
      value = findProperty.call([REPL_SERVICES, svc, key[0]])
    end
    
    value
  end
  
  # Get the config file value for a property. 
  def getConfigFileProperty(key, transform_values_method)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    findProperty = lambda do |keys|
      key_string = keys.join('.')
      if @in_config_file_prompt_handler[key_string] == true
        return nil
      end

      begin
        @in_config_file_prompt_handler[key_string] = true

        value = getPromptHandler().get_config_file_property(keys, transform_values_method)

        @in_config_file_prompt_handler[key_string] = false
      rescue IgnoreError
        @in_config_file_prompt_handler[key_string] = false
      rescue => e
        @in_config_file_prompt_handler[key_string] = false
        raise e
      end

      return value
    end
    
    value = findProperty.call(key)
    
    if value == nil && key.size == 1 && (host = getNestedProperty([DEPLOYMENT_HOST]))
      value = findProperty.call([HOSTS, host, key[0]])
    end
    
    if value == nil && key.size == 1 && (svc = getNestedProperty([DEPLOYMENT_SERVICE]))
      value = findProperty.call([REPL_SERVICES, svc, key[0]])
    end
    
    value
  end
  
  # Get the property value or return the default if nil
  def getPropertyOr(key, default = "")
    value = getProperty(key)
    if value == nil
      default
    else
      value
    end
  end
  
  # Set a property value. 
  def setProperty(key, value)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    setNestedProperty(value, key)
  end 
  
  # Set the property to a value only if it is currently unset. 
  def setDefault(key, value)
    if key.is_a?(String)
      key = key.split('.')
    end
    
    if getNestedProperty(key) == nil
      setNestedProperty(value, key)
    end
  end
  
  # Set multiple properties from a delimited string of key value pairs. 
  def setPropertiesFromList(list, delimiter)
    keyValuePairs = list.split(delimiter)
    keyValuePairs.each do |pair|
      if pair =~ /^(.*)=(.*)$/
        key = $1
        value = $2
        setProperty(key, value)
      end
    end
  end
  
  # Get the underlying hash table. 
  def hash()
    @props
  end
  
  def getPromptHandler
    unless @prompt_handler
      @prompt_handler = ConfigurePromptHandler.new(self)
    end
    
    @prompt_handler
  end
end
