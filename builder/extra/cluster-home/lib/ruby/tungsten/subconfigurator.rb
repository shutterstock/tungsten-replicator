# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

require 'tungsten/parameter_names'
require 'tungsten/validator'
require 'tungsten/exception'

# Base class for handling subconfiguration (partial configuration) of 
# Tungsten cluster. 
class SubConfigurator
  @@pressEnterMsg = "Press ENTER to continue or ^C following by ENTER to exit..."
  
  # Called after selecting DB type to check prerequisites.
  def pre_checks()
    puts "Not implemented yet"
  end
  
  # Edit configuration items used by this part of configuration. 
  def edit()
    puts "Not implemented yet"
  end
  
  # Perform pre-configuration checks.  You can fail here if there's 
  # something you don't like. 
  def pre_configure()
    puts "Not implemented yet"
  end
  
  # Perform configuration of relevant files. 
  def configure()
    puts "Not implemented yet"
  end
  
  # Perform post-configuration actions.  This includes starting services, 
  # etc. 
  def post_configure()
    puts "Not implemented yet"
  end
  
  # Offer post-configuration advice. 
  def print_usage()
    puts "Not implemented yet"
  end
  
  # TODO: Move following into a utility location. 
  
  # Execute a command with logging.
  def exec_cmd2(cmd, ignore_fail)
    puts("Executing command: " + cmd)
    successful = system(cmd)
    rc = $?
    puts("Success: #{successful} RC: #{rc}")
    if rc != 0 && ! ignore_fail
      raise SystemError, "Command failed: " + cmd
    end
  end
  
  # Convenience method for commands that are expected to succeed.
  def exec_cmd(cmd)
    exec_cmd2(cmd, false)
  end
  
  # Read a value from the command line. 
  def read_value(prompt, default)
    printf "%s [%s]: ", prompt, default
    value = STDIN.gets
    value.strip!
    if value == ""
      return default
    else
      return value
    end
  end
  
  # Find out the current user ID. 
  def whoami
    if ENV['USER']
      ENV['USER']
    elsif ENV['LOGNAME']
      ENV['LOGNAME']
    else
      `whoami`
    end
  end
  
  # Find out the current user ID. 
  def sudo_user()
    if ENV['SUDO_USER']
      ENV['SUDO_USER']
    else
        ""
    end
  end
  
end
