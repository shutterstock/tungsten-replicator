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
  def store(properties_filename)
    # Protect I/O with trap for Ctrl-C. 
    interrupted = false
    old_trap = trap("INT") {
      interrupted = true;
    }
    
    # Write. 
    File.open(properties_filename, 'w') do |file|
      file.printf "# Tungsten configuration properties\n"
      file.printf "# Date: %s\n", DateTime.now
      file.printf JSON.pretty_generate(@props)
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
  
  # Get a property value. 
  def getProperty(key) 
    @props[key]
  end
  
  # Get the property value or return the default if nil
  def getPropertyOr(key, default)
    value = getProperty(key)
    if value == nil
      default
    else
      value
    end
  end
  
  # Set a property value. 
  def setProperty(key, value) 
    if value == nil
      @props.delete(key)
    else
      @props[key] = value
    end
  end 
  
  # Set the property to a value only if it is currently unset. 
  def setDefault(key, value)
    if ! @props[key]
      setProperty(key, value)
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
end
