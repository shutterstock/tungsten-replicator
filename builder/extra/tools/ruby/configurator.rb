#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

# System libraries.
require 'system_require'

system_require 'singleton'
system_require 'optparse'
system_require 'ostruct'
system_require 'date'
system_require 'fileutils'
system_require 'socket'
system_require 'logger'
system_require 'tempfile'
system_require 'uri'
system_require 'resolv'

# This isn't required, but it makes the output update more often with SSH results
begin
  require 'net/ssh'
rescue LoadError
end

system_require 'json'
system_require 'transformer'
system_require 'validator'
system_require 'properties'
system_require 'configure/is_tools_package'
system_require 'configure/parameter_names'
system_require 'configure/configure_messages'
system_require 'configure/configure_module'
system_require 'configure/configure_package'
system_require 'configure/configure_prompt_handler'
system_require 'configure/configure_prompt_interface'
system_require 'configure/configure_prompt'
system_require 'configure/group_configure_prompt'
system_require 'configure/configure_validation_handler'
system_require 'configure/validation_check_interface'
system_require 'configure/configure_validation_check'
system_require 'configure/group_validation_check'
system_require 'configure/configure_deployment_handler'
system_require 'configure/configure_deployment'

DEFAULTS = "__defaults__"

DBMS_MYSQL = "mysql"
DBMS_POSTGRESQL = "postgresql"

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

Dir[File.dirname(__FILE__) + '/configure/packages/*.rb'].each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end
Dir[File.dirname(__FILE__) + '/configure/modules/*.rb'].each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end
Dir[File.dirname(__FILE__) + '/configure/deployments/*.rb'].each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end

# Manages top-level configuration.
class Configurator
  include Singleton
  
  attr_reader :package
  
  TUNGSTEN_COMMUNITY = "Community"
  TUNGSTEN_ENTERPRISE = "Enterprise"
  
  CLUSTER_CONFIG = "cluster.cfg"
  HOST_CONFIG = "tungsten.cfg"
  TEMP_DEPLOY_DIRECTORY = "tungsten_configure"
  TEMP_DEPLOY_HOST_CONFIG = ".tungsten.cfg"
  CURRENT_RELEASE_DIRECTORY = "tungsten"
  
  SERVICE_CONFIG_PREFIX = "service_"
  
  # Initialize configuration arguments.
  def initialize()
    # Set instance variables.
    @remote_messages = []
    @package = ConfigurePackageCluster.new(@config)
    
    # This is the configuration object that will be stored
    @config = Properties.new

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.output_threshold = Logger::INFO
    @options.force = false
    @options.interactive = true
    @options.advanced = false
    @options.stream_output = false
    @options.display_help = false
    @options.validate_only = false

    if is_full_tungsten_package?()
      configs_path = File.expand_path(ENV['PWD'] + "/../../configs/")
    else
      configs_path = false
    end
    # Check for the tungsten.cfg in the unified configs directory
    if configs_path && File.exist?("#{configs_path}/#{CLUSTER_CONFIG}")
      @options.config = "#{configs_path}/#{CLUSTER_CONFIG}"
    else
      @options.config = "#{get_base_path()}/#{CLUSTER_CONFIG}"
    end
    
    if is_full_tungsten_package?() && !File.exist?(@options.config)
      if configs_path && File.exist?("#{configs_path}/#{HOST_CONFIG}")
        @options.config = "#{configs_path}/#{HOST_CONFIG}"
      elsif File.exist?("#{get_base_path()}/#{HOST_CONFIG}")
        @options.config = "#{get_base_path()}/#{HOST_CONFIG}"
      end
    end
  end

  # The standard process, collect prompt values, validate on each host
  # then deploy on each host
  def run
    unless parsed_options?(ARGV)
      output_usage()
      exit
    end
    
    unless use_streaming_ssh()
      warning("It is recommended that you install the net-ssh rubygem")
    end
    
    write_header "Tungsten #{tungsten_version()} Configuration Procedure"
    display_help()
    
    prompt_handler = ConfigurePromptHandler.new(@config)
    
    # If not running in batch mode, collect responses for configuration prompts
    if @options.interactive
      # Collect responses to the configuration prompts
      begin
        unless prompt_handler.run()
          write_header("There are errors with the values provided in the configuration file", Logger::ERROR)
          prompt_handler.print_errors()
          exit 1
        end
        
        save_prompts()
        
        value = ""
        while value.to_s == ""
          puts "
Tungsten has all values needed to configure itself properly.  
Do you want to continue with the configuration (Y) or quit (Q)?"
          value = STDIN.gets
          value.strip!
          
          case value.to_s().downcase()
            when "y"
              next
            when "yes"
              next 
            when "no"
              raise ConfigureSaveConfigAndExit
            when "n"
              raise ConfigureSaveConfigAndExit
            when "q"
              raise ConfigureSaveConfigAndExit
            when "quit"
              raise ConfigureSaveConfigAndExit
            else
              value = nil
          end
        end
      rescue ConfigureSaveConfigAndExit => csce 
        write "Saving configuration values and exiting"
        save_prompts()
        exit 0
      end
    else
      # Validate the values in the configuration file against the prompt validation
      unless prompt_handler.is_valid?()
        write_header("There are errors with the values provided in the configuration file", Logger::ERROR)
        prompt_handler.print_errors()
        exit 1
      end
    end
    
    @package.post_prompt_handler_run()
    
    deployment_method = get_deployment()
    
    # Make sure that basic connectivity to the hosts works
    unless deployment_method.prevalidate()
      exit 1
    end
    
    # Copy over configuration script code and the host configuration file
    unless deployment_method.prepare()
      exit 1
    end
    
    unless @options.force
      unless deployment_method.validate()
        write_header("Validation failed", Logger::ERROR)
        deployment_method.get_validation_handler().output_errors()
        
        exit 1
      end
      
      info("")
      info("Validation finished")
    end
    
    unless @options.validate_only
      # Execute the deployment of each configuration object for the deployment
      unless deployment_method.deploy()
        write_header("Deployment failed", Logger::ERROR)
        deployment_method.get_deployment_handler().output_errors()
      
        exit 1
      end
    
      info("")
      info("Deployment finished")
    end
  end
  
  # Handle the remote side of the validation_handler->run function
  def validate
    unless parsed_options?(ARGV)
      output_usage()
      exit
    end
    
    # Outputting directly will break the serialized return string
    if has_tty?() && @options.stream_output == false
      info("")
      write_header "Validate #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
    end
    
    # Run the validation checks for a single configuration object
    result = get_deployment().validate_config(@config)
    if Configurator.instance.has_tty?() && @options.stream_output == false
      result.output()
    else
      result.messages = @remote_messages
      
      # Remove the config object so that the dump/load process is faster
      @config = nil
      puts Marshal.dump(result)
    end
  end
  
  # Handle the remote side of the deployment_handler->run function
  def deploy
    unless parsed_options?(ARGV)
      output_usage()
      exit
    end
    
    # Outputting directly will break the serialized return string
    if has_tty?() && @options.stream_output == false
      info("")
      write_header "Deploy #{@config.getProperty(HOST)}:#{@config.getProperty(HOME_DIRECTORY)}"
    end
    
    # Run the deploy steps for a single configuration object
    result = get_deployment().deploy_config(@config)
    if has_tty?() && @options.stream_output == false
      result.output()
    else
      result.messages = @remote_messages
      
      # Remove the config object so that the dump/load process is faster
      @config = nil
      puts Marshal.dump(result)
    end
  end
  
  # Locate and initialize deployments in the configure/deployments directory
  def initialize_deployments
    @deployments = []
    ConfigureDeployment.subclasses().each {
      |deployment_class|
      deployment = deployment_class.new()
      deployment.set_config(@config)
      @deployments << deployment
    }
    @deployments = @deployments.sort{|a,b| a.get_weight <=> b.get_weight}
  end
  
  def get_deployment
    get_deployments().each{
      |deployment|
      if deployment.include_deployment_for_package?(@package)
        if deployment.get_name() == @config.getProperty(DEPLOYMENT_TYPE)
          return deployment
        end
      end
    }
    
    raise "Unable to find a matching deployment method"
  end
  
  def get_deployments
    unless @deployments.is_a?(Array)
      initialize_deployments()
    end

    @deployments
  end
  
  # Parse command line arguments.
  def parsed_options?(arguments)
    opts=OptionParser.new
    
    # Needed again so that an exception isn't thrown
    opts.on("-p", "--package String") {|klass|
      begin
        unless defined?(klass)
          raise "Unable to find the #{klass} package"
        end
        
        @package = Module.const_get(klass).new(@config)
        
        unless @package.is_a?(ConfigurePackage)
          raise "Package '#{klass}' does not extend ConfigurePackage"
        end
      rescue => e
        error("Unable to instantiate package: #{e.to_s()}")
        return false
      end
    }
    
    opts.on("-a", "--advanced")       {|val| @options.advanced = true}
    opts.on("-b", "--batch")          {|val| @options.interactive = false}
    opts.on("-i", "--interactive")    {|val| @options.interactive = true}
    opts.on("-c", "--config String")  {|val| @options.config = val }
    opts.on("-h", "--help")           {|val| @options.display_help = true }
    opts.on("-q", "--quiet")          {@options.output_threshold = Logger::WARN}
    opts.on("-n", "--info")           {@options.output_threshold = Logger::INFO}
    opts.on("-v", "--verbose")        {@options.output_threshold = Logger::DEBUG}
    opts.on("--no-validation")        {|val| @options.force = true }
    opts.on("--validate-only")        {@options.validate_only = true}
    
    # Argument used by the validation and deployment handlers
    opts.on("--stream")               {@options.stream_output = true }

    begin
      opts.order!(arguments)
    rescue OptionParser::InvalidOption => io
      # Prepend the invalid option onto the arguments array
      arguments = io.recover(arguments)
    rescue => e
      if @options.display_help
        output_help
        exit 0
      end
      
      error("Argument parsing failed: #{e.to_s()}")
      return false
    end
    
    if @options.display_help
      output_help
      exit 0
    end

    unless arguments_valid?()
      return false
    end
    
    begin
      unless @package.parsed_options?(arguments)
        return false
      end
    rescue => e
      error(e.to_s())
      return false
    end
    
    true
  end

  # True if required arguments were provided
  def arguments_valid?
    if @options.interactive
      # For interactive mode, must be able to write the config file.
      if File.exist?(@options.config)
        if ! File.writable?(@options.config)
          write "Config file must be writable for interactive mode: #{@options.config}", Logger::ERROR
          return false
        end
      else
        if ! File.writable?(File.dirname(@options.config))
          write "Config file directory must be writable for interactive mode: #{@options.config}", Logger::ERROR
          return false
        end
      end
    else
      # For batch mode, options file must be readable.
      if ! File.readable?(@options.config) && File.exist?(@options.config)
        write "Config file is not readable: #{@options.config}", Logger::ERROR
        return false
      end
    end
    
    # Load the current configuration values
    if File.exist?(@options.config)
      @config.load(@options.config)
    end
    
    true
  end
  
  def save_prompts
    if @package.store_config_file?
      temp = @package.prepare_saved_config(@config)
      temp.store(@options.config)
    end
  end

  def output_help
    output_version
    output_usage
  end

  def output_usage
    @package.output_usage()
  end

  def output_version
    write "#{File.basename(__FILE__)} version #{VERSION}"
  end
  
  def display_help
    filename = File.dirname(__FILE__) + "/configure/interface_text/configure_run"
    write_from_file(filename)
  end
  
  # Write a header
  def write_header(content, level=Logger::INFO)
    unless enable_log_level(level)
      return
    end
    
    if enable_output?()
      puts "#####################################################################"
      puts "# #{content}"
      puts "#####################################################################"
    else
      @remote_messages << "#####################################################################"
      @remote_messages << "# #{content}"
      @remote_messages << "#####################################################################"
    end
  end

  # Write a sub-divider, which is used between sections under a singl header.
  def write_divider(level=Logger::INFO)
    unless enable_log_level(level)
      return
    end
    
    if enable_output?()
      puts "---------------------------------------------------------------------"
    else
      @remote_messages << "---------------------------------------------------------------------"
    end
  end
  
  def write(content="", level=Logger::INFO, hostname = nil, force = false)
    if !enable_log_level(level) && force == false
      return
    end
    
    unless content == ""
      content = get_log_level_prefix(level, hostname) + content
    end
    
    if enable_output?()
      puts content
      $stdout.flush()
    else
      @remote_messages << content
    end
  end
  
  def write_from_file(filename, level=Logger::INFO)
    unless enable_log_level(level)
      return
    end
    
    f = File.open(filename, "r") 
    f.each_line do |line|
      if enable_output?()
        puts line
      else
        @remote_messages << line
      end
    end
    f.close
  end
  
  def info(message, hostname = nil)
    write(message, Logger::INFO, hostname)
  end
  
  def warning(message, hostname = nil)
    write(message, Logger::WARN, hostname)
  end
  
  def error(message, hostname = nil)
    write(message, Logger::ERROR, hostname)
  end
  
  def exception(e, hostname = nil)
    error(e.to_s() + ":\n" + e.backtrace.join("\n"), hostname)
  end
  
  def debug(message, hostname = nil)
    write(message, Logger::DEBUG, hostname)
  end
  
  def get_log_level_prefix(level=Logger::INFO, hostname = nil)
    case level
    when Logger::ERROR then prefix = "ERROR"
    when Logger::WARN then prefix = "WARN "
    when Logger::DEBUG then prefix = "DEBUG"
    else
      prefix = "INFO "
    end
    
    if hostname == nil
      hostname = @config.getProperty(HOST)
    end
    
    if hostname == nil
      "#{prefix} >> "
    else
      "#{prefix} >> #{hostname} >> "
    end
  end
  
  def enable_log_level(level=Logger::INFO)
    if level<@options.output_threshold
      false
    else
      true
    end
  end
  
  # Find out the current user ID.
  def whoami
    if ENV['USER']
      ENV['USER']
    elsif ENV['LOGNAME']
      ENV['LOGNAME']
    else
      `whoami`.chomp
    end
  end
  
  def hostname
    `hostname`.chomp
  end
  
  def get_base_path
    if is_full_tungsten_package?()
      File.expand_path(File.dirname(__FILE__) + "/../../")
    else
      File.expand_path(File.dirname(__FILE__) + "/../")
    end
  end
  
  def get_ruby_prefix
    if is_full_tungsten_package?()
      "tools/ruby"
    else
      "ruby"
    end
  end
  
  def get_basename
    File.basename(get_base_path())
  end
  
  def has_tty?
    (`tty > /dev/null 2>&1; echo $?`.chomp == "0")
  end
  
  def enable_output?
    (has_tty?() || @options.stream_output == true)
  end
  
  def is_full_tungsten_package?
    (IS_TOOLS_PACKAGE==false)
  end
  
  def get_manifest_file_path
    "#{get_base_path()}/.manifest"
  end
  
  def get_release_details
    unless @release_details
      # Read manifest to find build version. 
      begin
        File.open(get_manifest_file_path(), 'r') do |file|
          file.read.each_line do |line|
            line.strip!
            if line =~ /RELEASE:\s*tungsten-(community|enterprise|replicator)-([0-9a-zA-Z\.]+)[-]?([0-9a-zA-Z\-\.]*)/
              $release_name = "tungsten-#{$1}-#{$2}"
              unless $3 == ""
                $release_name = $release_name + "-#{$3}"
              end
              
              @release_details = {
                "is_enterprise_package" => ($1 == "enterprise"),
                "version" => $2,
                "variant" => $3,
                "name" => $release_name
              }
            end
          end
        end
      rescue Exception => e
        raise "Unable to read .manifest file: #{e.to_s}"
      end
    end
    
    if @release_details
      @release_details
    else
      raise "Unable to determine the current release version"
    end
  end
  
  def get_release_name
    release_details = get_release_details()
    release_details["name"]
  end
  
  def advanced_mode?
    @options.advanced == true
  end
  
  def is_interactive?
    (@options.interactive == true)
  end
  
  def is_enterprise?
    release_details = get_release_details()
    release_details["is_enterprise_package"]
  end
  
  def tungsten_version
    unless is_enterprise?()
      TUNGSTEN_COMMUNITY
    else
      TUNGSTEN_ENTERPRISE
    end
  end
  
  def get_config_filename
    File.expand_path(@options.config)
  end
  
  def get_startup_script_filename(shell)
    case shell
    when "/bin/bash"
      ".bashrc"
    when "/bin/csh"
      ".cshrc"
    when "/bin/ksh"
      ".kshrc"
    when "/bin/sh"
      ".profile"
    when "/bin/tcsh"
      ".tcshrc"
    else
      ""
    end
  end
  
  # Is the Net::SSH module available
  def use_streaming_ssh()
    if defined?(Net::SSH)
      true
    else
      false
    end
  end
  
  def os?
    os = `uname -s`.chomp
    case
      when os == "Linux" then OS_LINUX
      when os == "Darwin" then OS_MACOSX
      when os == "SunOS" then OS_SOLARIS
      else OS_UNKNOWN
    end
  end 
  
  def arch?
    # Architecture is unknown by default.
    arch = `uname -m`.chomp
    case
      when arch == "x86_64" then OS_ARCH_64
      when arch == "i386" then OS_ARCH_32
      when arch == "i686" then OS_ARCH_32
      else OS_ARCH_UNKNOWN
    end
  end

  def distro?
    # If the OS is unknown accept it only if this is a forced configuration.
    case os?()
      when OS_UNKNOWN
        raise "Operating system could not be determined"
      when OS_LINUX
        if File.exist?("/etc/redhat-release")
          OS_DISTRO_REDHAT
        elsif File.exist?("/etc/debian_version")
          @options.distro = OS_DISTRO_DEBIAN
        else
          OS_DISTRO_UNKNOWN
        end
    end
  end
  
  def can_install_services_on_os?
    # If the OS is unknown accept it only if this is a forced configuration.
    case os?()
      when OS_UNKNOWN
        raise "Operating system could not be determined"
      when OS_LINUX
        if File.exist?("/etc/redhat-release")
          true
        elsif File.exist?("/etc/debian_version")
          true
        else
          false
        end
    end
  end
end

def cmd_result(command, ignore_fail = false)
  Configurator.instance.debug("Execute `#{command}`")
  result = `#{command} 2>&1`.chomp
  rc = $?
  
  if rc != 0 && ! ignore_fail
    raise CommandError.new(command, rc, result)
  else
    Configurator.instance.debug("RC: #{rc}, Result: #{result}")
  end
  
  return result
end

# The user has requested to save all current configuration values and exit
class ConfigureSaveConfigAndExit < StandardError
end

# The user has requested to accept the default value for the current and
# all remaining prompts
class ConfigureAcceptAllDefaults < StandardError
end

# The user has requested to return the previous prompt
class ConfigurePreviousPrompt < StandardError
end

class CommandError < StandardError
  attr_reader :command, :rc, :result
  
  def initialize(command, rc, result)
    @command = command
    @rc = rc
    @result = result
    
    super(build_message())
  end
  
  def build_message
    "Failed: #{command}, RC: #{rc}, Result: #{result}"
  end
end

class RemoteCommandError < CommandError
  attr_reader :user, :host
  
  def initialize(user, host, command, rc, result)
    @user = user
    @host = host
    super(command, rc, result)
  end
  
  def build_message
    "Failed: #{command}, RC: #{rc}, Result: #{result}"
  end
end