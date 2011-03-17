#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

# Super class for environment tests run as part of validation.  This class
# contains properties and helper methods for an environment test.  Tests
# subclass this class and supply at least the title, description, and 
# validate methods. 
class EnvironmentValidator
  # Set validator constants. 
  SUCCESS = "OK"
  FAILURE = "FAILURE"

  # Define r/w and r/o accessors for properties. 
  attr_accessor :result, :result_message
  attr_reader :title, :description, :warnings

  # Subclasses must implement this. 
  def validate()
    raise "No validation routine!"
  end

  # Add a warning.  We need to initialize the array the first time through. 
  def add_warning(message)
    unless @warnings
      @warnings = []
    end
    @warnings << message
  end

  # Record a validation success.  This should be at 
  def validation_success(message)
    @result = SUCCESS
    @result_message = message
  end

  # Record a validation failure.
  def validation_failure(message)
    @result = FAILURE
    @result_message = message
  end
end

# 
# OPERATING SYSTEM VALIDATORS
#

# Validator for operating system. 
class OSValidator < EnvironmentValidator
  # Define operating system names.
  OS_LINUX = "linux"
  OS_MACOSX = "macosx"
  OS_SOLARIS = "solaris"
  OS_UNKNOWN = "unknown"

  OS_DISTRO_REDHAT = "distro_redhat"
  OS_DISTRO_DEBIAN = "distro_debian"
  OS_DISTRO_UNKNOWN = "distro_unknown"

  OS_ARCH_32 = "32-bit"
  OS_ARCH_64 = "64-bit"
  OS_ARCH_UNKNOWN = "unknown"

  def initialize(options)
    @title = "OS version check"
    @description = "Check operating system and distribution to ensure it is supported"
  end

  # Figure out OS and ensure it is supported. 
  def validate()
    # Check operating system.
    puts "Checking operating system type"
    uname = `uname -a`.chomp
    puts "Operating system: #{uname}"
    uname_s = `uname -s`.chomp
    os = case
    when uname_s == "Linux" then OS_LINUX
    when uname_s == "Darwin" then OS_MACOSX
    when uname_s == "SunOS" then OS_SOLARIS
    else OS_UNKNOWN
    end
    if os == OS_UNKNOWN
      validation_failure "Could not determine OS!"
      return
    elsif os == OS_MACOSX
      add_warning "Mac OS X is provisionally supported only"
    end

    # Architecture is unknown by default.
    puts "Checking processor architecture" 
    uname_m = `uname -m`.chomp
    arch = case
    when uname_m == "x86_64" then OS_ARCH_64
    when uname_m == "i386" then OS_ARCH_32
    when uname_m == "i686" then OS_ARCH_32
    else
      OS_ARCH_UNKNOWN
    end
    if arch == OS_ARCH_UNKNOWN
      validation_error "Could not determine OS architecture!"
      return
    elsif arch == OS_ARCH_32
      add_warning "32-bit architecture not recommended for DBMS nodes"
    end

    # Report on Linux distribution.
    if os == OS_LINUX
      puts "Checking Linux distribution" 
      if File.exist?("/etc/redhat-release")
        system "cat /etc/redhat-release"
      elsif File.exist?("/etc/debian_version")
        system "cat /etc/debian_version"
      else
        validation_failure "Could not determine Linux distribution!"
        return  
      end
    end

    validation_success "Supported operating system found"
  end
end

# Validator for the Ruby version. 
class RubyValidator < EnvironmentValidator
  def initialize(options)
    @title = "Ruby version check"
    @description = "Check Ruby version to ensure it is supported"
  end

  # Ensure Ruby version is supported. 
  def validate()
    ruby_version = RUBY_VERSION
    puts "Ruby version: #{ruby_version}"
    if ruby_version =~ /^1\.8\.[5-9]/
      validation_success "Ruby version OK"
    elsif ruby_version =~ /^1\.8/
      validation_failure "Ruby version must be at least 1.8.5"
    elsif ruby_version =~ /^1\.9/
      validation_warning "Ruby version may not work; try Ruby 1.8.5-1.8.7"
    else
      validation_failure "Unrecognizable Ruby version: #{ruby_version}"
    end
  end
end

# Validator for Java.  
class JavaValidator < EnvironmentValidator
  def initialize(options)
    @title = "Java version check"
    @description = "Check Java version to ensure it is present and supported"
  end

  # Ensure we can find Java and that it has a supported version. 
  def validate()
    # Look for Java.
    java_out = `java -version 2>&1`
    puts java_out
    if $? == 0
      if java_out =~ /Java|JDK/
        validation_success "Supported Java found"
      else
        validation_failure "Unknown Java version"
      end
    else
      validation_failure "Java binary not found in path"
    end
  end
end

# 
# NETWORK VALIDATORS
#

# Validator for hostname. 
class HostnameValidator < EnvironmentValidator
  def initialize(options)
    @title = "Hostname check"
    @description = "Ensure host name is legal host name, not localhost"
  end

  # Check the host name. 
  def validate()
    # Check operating system.
    hostname = `hostname`.chomp
    puts "Host name: #{hostname}"
    if hostname =~ /localhost\./
      validation_failure "Host name may not be localhost"
    else
      validation_success "Host name is OK"
    end
  end
end

# Validator for IP address. 
class IpAddressValidator < EnvironmentValidator
  def initialize(options)
    @title = "IP address check"
    @description = "Ensure IP address for host name is routable, not loopback"
  end

  # Check the IP address. 
  def validate()
    # Check operating system.
    hostname = `hostname --ip-address`.chomp
    puts "IP address(es): #{hostname}"
    if hostname =~ /localhost\./
      validation_failure "Host IP address(es) may not include loopback address"
    else
      validation_success "Host IP address(es) OK"
    end
  end
end

# 
# MYSQL DATABASE VALIDATORS 
#

# Abstract validator with helper functions. 
class MysqlAbstractValidator < EnvironmentValidator
  # Execute mysql command and return result to client. 
  def mysql(user, password, command)
    # Ensure we have credentials.
    if ! user || ! password
      raise EnvironmentValidatorException, 
            "Dblogin and/or dbpassword missing; unable to login"
    end

    # Construct command and return output. 
    hostname = `hostname`.chomp
    mysql_cmd = "mysql -u#{user} -p#{password} -h#{hostname} -e \"#{command}\""
    puts "MySQL command: #{mysql_cmd}"
    `#{mysql_cmd}`
  end
end

# Ensure mysql client is present in environment. 
class MysqlClientValidator < MysqlAbstractValidator
  def initialize(options)
    @title = "Mysql client check"
    @description = "Ensure mysql client is present in the execution path"
  end

  # Check mysql client programs. 
  def validate()
    # Try to find mysql in path. 
    mysql = `which mysql 2> /dev/null`.chomp
    puts "MySQL client path: #{mysql}"
    if mysql == ""
      validation_failure "mysql program not found in execution path"
      return
    end

    # Execute to ensure it works. 
    mysql_version = `#{mysql} --version`.chomp
    if $? == 0
      puts "MySQL client version: #{mysql_version}"
      validation_success "Mysql client is OK"
    else
      validation_failure "Unable to execute mysql client"
    end
  end
end

# Ensure MySQL user and password work using current host name. 
class MysqlLoginValidator < MysqlAbstractValidator
  def initialize(options)
    # Need options for db login and password. 
    @options = options
    @title = "Database login check"
    @description = "Ensure MySQL database is available and accepts TCP/IP logins"
  end

  # Check login by selecting a well known string. 
  def validate()
    login_output = mysql(@options.dbuser, @options.dbpassword, "select 'ALIVE'")
    if login_output =~ /ALIVE/
      validation_success "MySQL server and login OK"
    else
      validation_failure "MySQL server is unavailable or login does not work"
    end
  end
end

# Ensure replication binlog is enabled (and check any other replication 
# prerequisites here).  
class MysqlReplicationValidator < MysqlAbstractValidator
  def initialize(options)
    # Need options for db login and password. 
    @options = options
    @title = "MySQL replication config check"
    @description = "Ensure MySQL replication is enabled and properly configured"
  end

  # Check login by selecting a well known string. 
  def validate()
    puts "Running SHOW MASTER STATUS to see if replication is enabled"
    show_master = mysql(@options.dbuser, @options.dbpassword, 
                        "show master status\\G")
    unless show_master =~ /File:/
      validation_failure "MySQL replication is not configured"
      puts "Output from SHOW MASTER STATUS:"
      puts show_master
    end

    # Warn if sync_binlog is not set. 
    puts "Checking sync_binlog setting"
    show_sync_binlog = mysql(@options.dbuser, @options.dbpassword, 
                       "show variables like 'sync_binlog'\\G")
    if show_sync_binlog =~ /Value:\s*0/
      add_warning "Set sync_binlog to 1 to avoid binlog corruption on crash"
    elsif show_sync_binlog =~ /Value:\s*1/
      # Do nothing. 
    else
      add_warning "Check on sync_binlog did not work - could not find value"
      puts show_sync_binlog
    end

    # Looks good. 
    validation_success "MySQL replication is configured"
  end
end

# Validator factory methods. 
class EnvironmentValidatorFactory
  # Return generic os environment validators. 
  def os_validators(options)
    [ OSValidator.new(options), 
      RubyValidator.new(options), 
      JavaValidator.new(options) ]
  end

  # Return network validators. 
  def network_validators(options)
    [ HostnameValidator.new(options),
      IpAddressValidator.new(options) ]
  end

  # Return MySQL validators. 
  def mysql_validators(options)
    [ MysqlClientValidator.new(options), 
      MysqlLoginValidator.new(options),
      MysqlReplicationValidator.new(options) ]
  end
end

# Exception to identify a environment validation error. 
class EnvironmentValidatorException < RuntimeError
end
