#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Manages store and load of installation properties. 

require 'tungsten/system_require'
system_require 'date'

# Super class for property validators. 
class PropertyValidator
  def initialize(regex, message) 
    @regex = Regexp.new(regex)
    @message = message
  end
  def validate(value)
    if value =~ @regex
      return value
    end
    raise PropertyValidatorException, @message + ": " + value.to_s, caller
  end
end

# File property validator.  Type must be "file" or "directory". 
class FilePropertyValidator 
  def initialize(ftype, writable, message, create)
    if ftype != "file" && ftype != "directory" 
      raise RuntimeError, "Invalid type for file checking: " + type
    end

    @ftype = ftype
    @writable = writable
    @message = message
    @create = create
  end
  def validate(value)
    # Ensure the file or directory exists. 
    ok = true
    if @ftype == "file" && ! File.file?(value)
      ok = false
    elsif @ftype == "directory" && ! File.exists?(value)
      if @create
        if confirm "Directory #{value} does not exist, ok to create it?", "yes"
          Dir.mkdir(value)
          if File.exists?(value)
            return value
          else
            puts "Unable to create directory!"
          end
        end
      end
      ok = false
    elsif @ftype == "directory" && ! File.directory?(value)
      ok = false
    elsif ! File.readable?(value)
      ok = false
    elsif @writable && ! File.writable?(value)
      ok = false
    else
      return value
    end

    # Desired directory/file does not exist and/or could not be created. 
    raise PropertyValidatorException, @message + ": " + value, caller
  end

  def confirm(prompt, default)
    while true
      printf "%s [%s]: ", prompt, default
      value = STDIN.gets
      value.strip!
      if value == ""
        value = default
      end
      if value =~ /yes|YES|y|Y/
        return true
      elsif value =~ /no|NO|n|N/
        return false
      end
    end
  end
end

# Integer range validator.  Note that range value are assumed to be 
# strings. 
class IntegerRangeValidator < PropertyValidator
  def initialize(low, high, message) 
    super('^[0-9]+$', message)
    if low > high
      raise RuntimeError, "Invalid integer range: low=" + low + " high=" + high
    end
    @low = low.to_i
    @high = high.to_i
  end
  def validate(value)
    super(value)
    v_int = value.to_i
    if v_int >= @low && v_int <= @high
      return value
    else
    raise PropertyValidatorException, @message + ": " + value, caller
  end
end
end

# Define standard validators. 
PV_INTEGER = PropertyValidator.new('^[0-9]+$', "Value must be an integer")
PV_BOOLEAN = PropertyValidator.new('true|false', "Value must be true or false")
PV_IDENTIFIER = PropertyValidator.new('[A-Za-z0-9_]+', 
  "Value must consist only of letters, digits, and underscore (_)")
PV_HOSTNAME = PropertyValidator.new('[A-Za-z0-9_.]+', 
  "Value must consist only of letters, digits, underscore (_) and periods")
PV_DBMSTYPE = PropertyValidator.new("mysql|postgresql|none", 
  "Value must be a database:  mysql, postgresql, or none")
PV_BINLOG_EXTRACTION = PropertyValidator.new("direct|relay", 
  "Value must be direct or relay")
PV_LOGTYPE = PropertyValidator.new("dbms|disk", 
  "Value must be a supported replicator log type:  dbms (store in db) or disk")
PV_DEPLOYMENTTYPE = PropertyValidator.new("small|medium|large", 
  "Value must be small, medium, or large")
PV_DBMSROLE = PropertyValidator.new("master|slave|direct|dummy", 
  "Value must be one of the following: master, slave, direct, dummy")
PV_GC_MEMBERSHIP_PROTOCOL = PropertyValidator.new("multicast|gossip|ping", 
  "Value must be multicast, gossip or ping")
PV_POLICY_MGR_MODE = PropertyValidator.new("manual|automatic", 
  "Value must be manual or automatic")
PV_READABLE_DIR = FilePropertyValidator.new("directory", false, 
  "Value must be a readable directory", false)
PV_WRITABLE_DIR = FilePropertyValidator.new("directory", true, 
  "Value must be a writable directory", false)
PV_WRITABLE_OUTPUT_DIR = FilePropertyValidator.new("directory", true, 
  "Value must be a writable directory", true)
PV_EXECUTABLE_FILE = FilePropertyValidator.new("file", false, 
  "Value must be an executable file", false)
PV_READABLE_FILE = FilePropertyValidator.new("file", false,
  "Value must be an readable file", false)
PV_WRITABLE_FILE = FilePropertyValidator.new("file", true,
  "Value must be a writable file", false)
PV_ANY = PropertyValidator.new('.*', "Value may be any string")
PV_JAVA_MEM_SIZE = IntegerRangeValidator.new(128, 2048, 
  "Java heap size must be between 128 and 2048")
PV_REPL_BUFFER_SIZE = IntegerRangeValidator.new(1, 100, 
  "Replication transaction buffer size must be between 1 and 100")

# Provides metadata for a property file entry. 
class PropertyDescriptor
  attr_reader :desc, :key, :value, :default
  @validator = nil

  # Initialize descriptor with a block to perform checks. 
  def initialize(desc, validator, key, default)
    @desc = desc
    @validator = validator
    @key = key
    @default = default
  end

  # Check the value and return true if accepted. 
  def accept(value)
    @value = @validator.validate value
  end  
end

# Exception to identify a property validation error. 
class PropertyValidatorException < RuntimeError
end
