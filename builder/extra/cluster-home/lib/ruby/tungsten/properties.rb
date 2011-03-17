#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Manages store and load of installation properties. 

require 'tungsten/system_require'
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
    new_props = {}
    File.open(properties_filename, 'r') do |file|
      file.read.each_line do |line|
        line.strip!
        if (line =~ /^([\w\.]+)\s*=\s*(\S.*)/)
          key = $1
          value = $2
          new_props[key] = value
        end
      end 
    end
    @props = new_props
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
      @props.sort.each do | key, value |
        file.printf "%s=%s\n", key, value
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
  
  # Get a property value. 
  def getProperty(key) 
    @props[key]
  end 
  
  # Set a property value. 
  def setProperty(key, value) 
    @props[key] = value
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
