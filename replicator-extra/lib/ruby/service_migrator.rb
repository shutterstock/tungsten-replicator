#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

require 'tungsten/system_require'

# System libraries.
system_require 'optparse'
system_require 'ostruct'

require 'tungsten/properties'

class ServiceMigrator
  attr_reader :options
  
  # Initialize configuration arguments.
  def initialize(arguments, stdin)
    # Set instance variables.
    @arguments = arguments
    @stdin = stdin

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.CLUSTER_HOSTS = nil
    
    # The base directory for the cluster
    @options.CLUSTER_DIR=File.expand_path(File.dirname(__FILE__) + "/../../..")
    
    unless parsed_options?
      raise "Unable to parse command options"
    end
    
    if @options.SERVICE_NAME == nil
      raise "You must specify a service name with the '-s' argument"
    end
    
    puts "Read service properties"
    # Read the default MySQL parameters from the replicator.properties file
    replicator_properties = Properties.new
    replicator_properties.load("#{@options.CLUSTER_DIR}/tungsten-replicator/conf/static-#{@options.SERVICE_NAME}.properties")
    
    if @options.MYSQL_USER == nil
      @options.MYSQL_USER = replicator_properties.getProperty("replicator.extractor.mysql.user").strip
      @options.MYSQL_USER.sub!(/\$\{[a-zA-Z0-9_\.]*\}/) { |subkey| replicator_properties.getProperty(subkey.strip[2..-2]) }
    end
    
    if @options.MYSQL_PASSWORD == nil
      @options.MYSQL_PASSWORD = replicator_properties.getProperty("replicator.extractor.mysql.password")
      @options.MYSQL_PASSWORD.sub!(/\$\{[a-zA-Z0-9_\.]*\}/) { |subkey| replicator_properties.getProperty(subkey.strip[2..-2]) }
    end
    
    if @options.SCHEMA_NAME == nil
      @options.SCHEMA_NAME = replicator_properties.getProperty("replicator.schema")
      @options.SCHEMA_NAME.sub!(/\$\{[a-zA-Z0-9_\.]*\}/) { |subkey| replicator_properties.getProperty(subkey.strip[2..-2]) }
    end
    
    if @options.RELAY_LOG_PATTERN == nil
      @options.RELAY_LOG_PATTERN = replicator_properties.getProperty("replicator.resourceLogPattern")
      @options.RELAY_LOG_PATTERN.sub!(/\$\{[a-zA-Z0-9_\.]*\}/) { |subkey| replicator_properties.getProperty(subkey.strip[2..-2]) }
    end
  end
  
  # Parse command line arguments.
  def parsed_options?
    opts=OptionParser.new
    opts.on("-h", "--help")           {|val| output_help; exit 0}
    opts.on("-s", "--service String")         {|val| @options.SERVICE_NAME = val}
    
    opts.parse!(@arguments) rescue
    begin
      puts "Argument parsing failed"
      return false
    end

    true
  end
  
  def output_help
    puts "Usage: migrate-service-mysql-tungsten -s{service_name}"
    puts "Usage: migrate-service-tungsten-mysql -s{service_name}"
  end
  
  def mysql_to_tungsten
    exec_mysql("STOP SLAVE sql_thread")
    exec_mysql("CREATE DATABASE IF NOT EXISTS #{@options.SCHEMA_NAME};")
    exec("#{get_trepctl_command()} -service #{@options.SERVICE_NAME} start")

    puts "Determine current relay log position"
    slave_relay_log_file = nil
    slave_relay_log_pos = nil
    
    # Check that this machine is currently a slave
    slave_status = exec_mysql("SHOW SLAVE STATUS\\G")
    if (slave_status.length > 0)        
      # Read the current log file, current log position and if MySQL replication is
      # still working.  Starting Tungsten on the master will cause replication to fail 
      # on the slave because the database schema is not replicated across.
      #
      # Once MySQL replication has failed, Tungsten replication can take over.
      slave_status.each{|line|
        slave_status_parts = line.split(":")
        case slave_status_parts[0].strip
        when "Relay_Log_File"
          slave_relay_log_file = slave_status_parts[1].strip
        when "Relay_Log_Pos"
          slave_relay_log_pos = slave_status_parts[1].strip
        when "Slave_SQL_Running"
          if slave_status_parts[1].strip == "Yes"
            exec_mysql("START SLAVE sql_thread")
            exec("echo \"yes\" | #{get_trepctl_command()} -service #{@options.SERVICE_NAME} stop")
            raise "Unable to proceed because the slave SQL thread did not stop"
          end
        when "Slave_IO_Running"
          if slave_status_parts[1].strip != "Yes"
            exec_mysql("START SLAVE sql_thread")
            exec("echo \"yes\" | #{get_trepctl_command()} -service #{@options.SERVICE_NAME} stop")
            raise "Unable to proceed because the slave IO thread is not running"
          end
        end
      }
    end
    
    # Raise an error if we cannot determine the slave position
    if (slave_relay_log_file == nil && slave_relay_log_pos == nil)
      exec_mysql("START SLAVE sql_thread")
      exec("echo \"yes\" | #{get_trepctl_command()} -service #{@options.SERVICE_NAME} stop")
      raise "Unable to determine current slave log position"
    else
      puts "MySQL application stopped at #{slave_relay_log_file}:#{slave_relay_log_pos}"
      # Bring the master online at the position of the slave and restore the policy
      exec("#{get_trepctl_command()} -service #{@options.SERVICE_NAME} online -from-event #{slave_relay_log_file}:#{slave_relay_log_pos}")
    end
  end
  
  def tungsten_to_mysql
    exec("#{get_trepctl_command()} -service #{@options.SERVICE_NAME} offline")
    exec("echo \"yes\" | #{get_trepctl_command()} -service #{@options.SERVICE_NAME} stop")
    exec_mysql("STOP SLAVE")

    # Determine the binary log position and enable MySQL replication at that point
    puts "Determine current relay log position"
    relay_log_file = nil
    relay_log_pos = nil
    eventid = exec_mysql("select eventid from #{@options.SCHEMA_NAME}.trep_commit_seqno order by seqno desc limit 1\\G")
    eventid.each{|line|
      if line =~ /eventid:/
        eventid_parts = line.split(/[:;]/)
        if eventid_parts[1].strip() != "NULL"
          relay_log_file = "#{@options.RELAY_LOG_PATTERN}.#{eventid_parts[1].strip()}"
          relay_log_pos = eventid_parts[2].strip().to_i()
        end
      end
    }
    
    puts "Determine last SQL thread relay log position"
    slave_relay_log_file = nil
    slave_relay_log_pos = nil
    
    # Check that this machine is currently a slave
    slave_status = exec_mysql("SHOW SLAVE STATUS\\G")
    if (slave_status.length > 0)        
      # Read the current log file, current log position and if MySQL replication is
      # still working.  Starting Tungsten on the master will cause replication to fail 
      # on the slave because the database schema is not replicated across.
      #
      # Once MySQL replication has failed, Tungsten replication can take over.
      slave_status.each{|line|
        slave_status_parts = line.split(":")
        case slave_status_parts[0].strip
        when "Relay_Log_File"
          slave_relay_log_file = slave_status_parts[1].strip()
        when "Relay_Log_Pos"
          slave_relay_log_pos = slave_status_parts[1].strip().to_i()
        end
      }
    end
    
    if slave_relay_log_file == nil
      raise "Unable to determine last slave relay log position"
    elsif relay_log_file == nil
      puts "Start MySQL SQL_THREAD from #{slave_relay_log_file}:#{slave_relay_log_pos}"
      exec_mysql("CHANGE MASTER TO RELAY_LOG_FILE='./#{slave_relay_log_file}', RELAY_LOG_POS=#{slave_relay_log_pos}; START SLAVE; DROP DATABASE #{@options.SCHEMA_NAME};")
    else
      relay_log_file_number = relay_log_file.split(".")[1].to_i()
      slave_relay_log_file_number = slave_relay_log_file.split(".")[1].to_i()
      
      use_slave_relay_info = false
      if slave_relay_log_file_number > relay_log_file_number
        use_slave_relay_info = true
      elsif slave_relay_log_file_number == relay_log_file_number
        if slave_relay_log_pos > relay_log_pos
          use_slave_relay_info = true
        end
      end
      
      if use_slave_relay_info == true
        puts "Start MySQL SQL_THREAD from #{slave_relay_log_file}:#{slave_relay_log_pos}"
        exec_mysql("CHANGE MASTER TO RELAY_LOG_FILE='./#{slave_relay_log_file}', RELAY_LOG_POS=#{slave_relay_log_pos}; START SLAVE; DROP DATABASE #{@options.SCHEMA_NAME};")
      else
        puts "Start MySQL SQL_THREAD from #{relay_log_file}:#{relay_log_pos}"
        exec_mysql("CHANGE MASTER TO RELAY_LOG_FILE='./#{relay_log_file}', RELAY_LOG_POS=#{relay_log_pos}; START SLAVE; DROP DATABASE #{@options.SCHEMA_NAME};")
      end
    end
  end
  
  def exec_mysql(command)
    puts "mysql> #{command}"
    
    `mysql -u#{@options.MYSQL_USER} -p#{@options.MYSQL_PASSWORD} -e "#{command}"`.strip.split("\n")
  end
  
  def exec(command)
    exec_ssh(nil, command)
  end
  
  def exec_ssh(host, command)
    unless host == nil
      puts "[#{host}] > #{command}"
      out = `ssh #{host} #{command}`.strip.split("\n")
    else
      puts "[localhost] > #{command}"
      out = `#{command}`.strip.split("\n")
    end
    
    out
  end
  
  def header(text)
    puts
    puts "#"
    puts "# #{text}"
    puts "#"
  end
  
  def get_trepctl_command
    "#{@options.CLUSTER_DIR}/tungsten-replicator/bin/trepctl"
  end
end