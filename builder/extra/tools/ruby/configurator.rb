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
system_require 'ifconfig'
system_require 'pp'
system_require 'timeout'
system_require 'cgi'
system_require 'net/ssh'
system_require 'json'
system_require 'transformer'
system_require 'validator'
system_require 'properties'
system_require 'configure/is_tools_package'
system_require 'configure/parameter_names'
system_require 'configure/configure_messages'
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
system_require 'configure/database_platform'

DEFAULTS = "__defaults__"

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

REPL_ROLE_M = "master"
REPL_ROLE_S = "slave"
REPL_ROLE_DI = "direct"

DISTRIBUTED_DEPLOYMENT_NAME = "regular"
UPDATE_DEPLOYMENT_NAME = "update_deployment"
DIRECT_DEPLOYMENT_HOST_ALIAS = "local"

DEFAULT_SERVICE_NAME = "default"

class IgnoreError < StandardError
end

Dir[File.dirname(__FILE__) + '/configure/packages/*.rb'].sort().each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end
Dir[File.dirname(__FILE__) + '/configure/modules/*.rb'].sort().each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end
Dir[File.dirname(__FILE__) + '/configure/deployments/*.rb'].sort().each do |file| 
  require File.dirname(file) + '/' + File.basename(file, File.extname(file))
end
Dir[File.dirname(__FILE__) + '/configure/dbms_types/*.rb'].sort().each do |file| 
  begin
    require File.dirname(file) + '/' + File.basename(file, File.extname(file))
  rescue IgnoreError
  end
end

# Manages top-level configuration.
class Configurator
  include Singleton
  
  attr_reader :package, :options
  
  TUNGSTEN_COMMUNITY = "Community"
  TUNGSTEN_ENTERPRISE = "Enterprise"
  
  CLUSTER_CONFIG = "cluster.cfg"
  HOST_CONFIG = "tungsten.cfg"
  TEMP_DEPLOY_HOST_CONFIG = ".tungsten.cfg"
  CURRENT_RELEASE_DIRECTORY = "tungsten"
  
  SERVICE_CONFIG_PREFIX = "service_"
  
  # Initialize configuration arguments.
  def initialize()
    # Set instance variables.
    @remote_messages = []
    
    # This is the configuration object that will be stored
    @config = Properties.new
    
    @package = ConfigurePackageCluster.new(@config)

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.output_threshold = Logger::INFO
    @options.force = false
    @options.batch = false
    @options.interactive = false
    @options.advanced = false
    @options.stream_output = false
    @options.display_help = false
    @options.display_preview = false
    @options.display_config_file_help = false
    @options.display_template_file_help = false
    @options.validate_only = false
    @options.no_validation = false
    @options.output_config = false
    @options.ssh_options = {}

    if is_full_tungsten_package?()
      configs_path = File.expand_path(ENV['PWD'] + "/../../configs/")
    else
      configs_path = false
    end
    
    # Check for the tungsten.cfg in the unified configs directory
    @options.config = "#{get_base_path()}/#{CLUSTER_CONFIG}"
    
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
    parsed_options?(ARGV)
    
    write_header "Tungsten #{tungsten_version()} Configuration Procedure"
    display_help()
    
    prompt_handler = ConfigurePromptHandler.new(@config)
    
    # If not running in batch mode, collect responses for configuration prompts
    if @options.interactive
      # Collect responses to the configuration prompts
      begin
        prompt_handler.run()
        
        save_prompts()
        
        unless prompt_handler.is_valid?()
          write_header("There are errors with the values provided in the configuration file", Logger::ERROR)
          prompt_handler.print_errors()
          
          unless forced?()
            exit 1
          end
        end
        
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
        
        unless forced?()
          exit 1
        end
      end
    end
    
    deployment_method = get_deployment()
    
    # Make sure that basic connectivity to the hosts works
    unless deployment_method.prevalidate()
      unless forced?()
        exit 1
      end
    end
    
    begin
      # Copy over configuration script code and the host configuration file
      unless deployment_method.prepare()
        unless forced?()
          raise
        end
      end
    
      unless @options.no_validation
        unless deployment_method.validate()
          write_header("Validation failed", Logger::ERROR)
          deployment_method.get_validation_handler().output_errors()
      
          unless forced?()
            raise
          end
        end
    
        info("")
        info("Validation finished")
      end
    
      unless @options.validate_only
        # Execute the deployment of each configuration object for the deployment
        unless deployment_method.deploy()
          write_header("Deployment failed", Logger::ERROR)
          deployment_method.get_deployment_handler().output_errors()
      
          unless forced?()
            raise
          end
        end
      end
      
      deployment_method.cleanup()
    rescue
      deployment_method.cleanup()
      exit 1
    end
    
    info("")
    info("Deployment finished")
  end
  
  # Handle the remote side of the validation_handler->run function
  def validate
    parsed_options?(ARGV, false)
    
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
    parsed_options?(ARGV, false)
    
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
      
      if deployment_class.subclasses()
        deployment_class.subclasses().each {
          |sub_deployment_class|
        
          deployment = sub_deployment_class.new()
          deployment.set_config(@config)
          @deployments << deployment
        }
      end
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
  def parsed_options?(arguments, include_package = true)
    opts=OptionParser.new
    
    arguments = arguments.map{|arg|
      newarg = ''
      arg.split("").each{|b| 
        unless b[0].to_i< 33 || b[0].to_i>127 then 
          newarg.concat(b) 
        end 
      }
      newarg
    }
    
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
    opts.on("-b", "--batch")          {|val| @options.batch = true }
    opts.on("-c", "--config String")  {|val| @options.config = val }
    opts.on("--config-file-help")          {
      @options.display_config_file_help = true
    }
    opts.on("--template-file-help")          {
      @options.display_template_file_help = true
    }
    opts.on("-f", "--force")          {@options.force = true}
    opts.on("-h", "--help")           {@options.display_help = true}
    opts.on("-p", "--preview")        {
      @options.display_help = true
      @options.display_preview = true
    }
    opts.on("-i", "--interactive")    {|val| @options.interactive = true}
    opts.on("-n", "--info")           {@options.output_threshold = Logger::INFO}
    opts.on("-q", "--quiet")          {@options.output_threshold = Logger::WARN}
    opts.on("-v", "--verbose")        {@options.output_threshold = Logger::DEBUG}
    opts.on("--no-validation")        {|val| @options.no_validation = true }
    opts.on("--output-config")        { @options.output_config = true }
    opts.on("--property String")      {|val|
                                        val_parts = val.split("=")
                                        if val_parts.length() !=2
                                          raise "Invalid value #{val} given for '--property'.  There should be a key/value pair joined by a single =."
                                        end

                                        last_char=val_parts[0][-1,1]
                                        if last_char == "+"
                                          Transformer.add_global_addition(val_parts[0][0..-2], val_parts[1])
                                        elsif ast_char == "~"
                                          Transformer.add_global_match(val_parts[0][0..-2], val_parts[1])
                                        else
                                          Transformer.add_global_replacement(val_parts[0], val_parts[1])
                                        end
                                      }
    opts.on("--skip-validation-check String")      {|val|
                                        ConfigureValidationHandler.mark_skipped_validation_class(val)
                                      }
    opts.on("--net-ssh-option String")  {|val|
                                        val_parts = val.split("=")
                                        if val_parts.length() !=2
                                          raise "Invalid value #{val} given for '--net-ssh-option'.  There should be a key/value pair joined by a single =."
                                        end

                                        @options.ssh_options[val_parts[0].to_sym] = val_parts[1]
                                      }
    opts.on("--validate-only")        {@options.validate_only = true}
    
    # Argument used by the validation and deployment handlers
    opts.on("--stream")               {@options.stream_output = true }

    remainder = run_option_parser(opts, arguments)
    
    if is_batch?()
      @package = ConfigurePackageCluster.new(@config)
    end

    unless arguments_valid?()
      unless display_help?()
        exit 1
      end
    end
    
    if @options.output_config
      @config.output
      exit 0
    end
    
    if include_package
      if is_batch?()
        warning('Running in batch mode will disable the install options')
      else
        begin
          unless @package.parsed_options?(remainder)
            error("There was a problem parsing the arguments")
            unless display_help?()
              exit 1
            end
          end
        rescue => e
          error(e.to_s() + e.backtrace().join("\n"))
          unless display_help?()
            exit 1
          end
        end
      end
    end
    
    if display_help?()
      unless display_config_file_help?() || display_template_file_help?()
        output_help
      end
      
      if display_config_file_help?()
        write_header('Config File Options')
        display_config_file_help()
      end
      
      if display_template_file_help?()
        write_header('Template File Options')
        display_template_file_help()
      end
      
      exit 0
    end
    
    true
  end

  # True if required arguments were provided
  def arguments_valid?
    if is_interactive?()
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
    elsif read_config_file?
      # For batch mode, options file must be readable.
      if ! File.readable?(@options.config) && File.exist?(@options.config)
        write "Config file is not readable: #{@options.config}", Logger::ERROR
        return false
      end
    end
    
    # Load the current configuration values
    if read_config_file?
      if File.exist?(@options.config)
        @config.load(@options.config)
      end
    end
    
    true
  end
  
  def read_config_file?
    (@package.read_config_file? || @options.output_config)
  end
  
  def run_option_parser(opts, arguments, allow_invalid_options = true, invalid_option_prefix = nil)
    remaining_arguments = []
    while arguments.size() > 0
      begin
        arguments = opts.order!(arguments)
        
        # The next argument does not have a dash so the OptionParser
        # ignores it, we will add it to the stack and continue
        if arguments.size() > 0 && (arguments[0] =~ /-.*/) == nil
          remaining_arguments << arguments.shift()
        end
      rescue OptionParser::InvalidOption => io
        if allow_invalid_options
          # Prepend the invalid option onto the arguments array
          remaining_arguments = remaining_arguments + io.recover([])
        
          # The next argument does not have a dash so the OptionParser
          # ignores it, we will add it to the stack and continue
          if arguments.size() > 0 && (arguments[0] =~ /-.*/) == nil
            remaining_arguments << arguments.shift()
          end
        else
          if invalid_option_prefix != nil
            io.reason = invalid_option_prefix
          end
          raise io
        end
      rescue => e
        if display_help?()
          output_help
          exit 0
        end
      
        error("Argument parsing failed: #{e.to_s()}")
        exit 1
      end
    end
    
    remaining_arguments
  end
  
  def save_prompts()
    if @options.config && @package.store_config_file?
      @config.store(@options.config)
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
  
  def display_config_file_help
    prompt_handler = ConfigurePromptHandler.new(@config)
    prompt_handler.output_config_file_usage()
  end
  
  def display_template_file_help
    prompt_handler = ConfigurePromptHandler.new(@config)
    prompt_handler.output_template_file_usage()
  end
  
  def display_help?(enabled = nil)
    if enabled != nil
      @options.display_help = enabled
    end
    
    return @options.display_help || @options.display_config_file_help || @options.display_template_file_help
  end
  
  def display_preview?
    return @options.display_preview
  end
  
  def display_config_file_help?(enabled = nil)
    if enabled != nil
      @options.display_config_file_help = enabled
    end
    
    return @options.display_config_file_help
  end
  
  def display_template_file_help?(enabled = nil)
    if enabled != nil
      @options.display_template_file_help = enabled
    end
    
    return @options.display_template_file_help
  end
  
  # Write a header
  def write_header(content, level=Logger::INFO)
    unless enable_log_level?(level)
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
    unless enable_log_level?(level)
      return
    end
    
    if enable_output?()
      puts "---------------------------------------------------------------------"
    else
      @remote_messages << "---------------------------------------------------------------------"
    end
  end
  
  def write(content="", level=Logger::INFO, hostname = nil, force = false)
    if !enable_log_level?(level) && force == false
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
    unless enable_log_level?(level)
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
  
  def enable_log_level?(level=Logger::INFO)
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
  
  def is_localhost?(hostname)
    if hostname == DEFAULTS
      return false
    end
    
    debug("Is '#{hostname}' the current host?")
    if hostname == hostname()
      return true
    end
    
    begin
      ip_addresses = Timeout.timeout(2) {
        Resolv.getaddresses(hostname)
      }
    rescue Timeout::Error
      raise "Unable to complete configuration because of a DNS timeout"
    rescue
      raise "Unable to determine the IP addresses for '#{hostname}'"
    end
    
    debug("Search ifconfig for #{ip_addresses.join(', ')}")
    ifconfig = IfconfigWrapper.new().parse()
    ifconfig.each{
      |iface|
        
      begin
        # Do a string comparison so that we only match the address portion
        iface.addresses().each{
          |a|
          if ip_addresses.include?(a.to_s())
            return true
          end
        }
      rescue ArgumentError
      end
    }
  
    false
  end
  
  def get_base_path
    if is_full_tungsten_package?()
      File.expand_path(File.dirname(__FILE__) + "/../../")
    else
      File.expand_path(File.dirname(__FILE__) + "/../")
    end
  end
  
  def get_package_path
    if is_full_tungsten_package?()
      get_base_path()
    else
      runtime_path = File.expand_path(get_base_path() + "/.runtime/" + get_release_name())
      if File.exists?(runtime_path)
        return runtime_path
      else
        return nil
      end
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
  
  def get_unique_basename
    get_basename() + "_pid#{Process.pid}"
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
  
  def is_batch?
    (@options.batch == true) && @package.allow_batch?()
  end
  
  def is_interactive?
    (@options.interactive == true) && @package.allow_interactive?()
  end
  
  def forced?
    (@options.force == true)
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
  
  # Determines if a shell command exists by searching for it in ENV['PATH'].
  def command_exists?(command)
    ENV['PATH'].split(File::PATH_SEPARATOR).any? {|d| File.exists? File.join(d, command) }
  end

  # Returns [width, height] of terminal when detected, nil if not detected.
  # Think of this as a simpler version of Highline's Highline::SystemExtensions.terminal_size()
  def detect_terminal_size
    unless @terminal_size
      if (ENV['COLUMNS'] =~ /^\d+$/) && (ENV['LINES'] =~ /^\d+$/)
        @terminal_size = [ENV['COLUMNS'].to_i, ENV['LINES'].to_i]
      elsif (RUBY_PLATFORM =~ /java/ || (!STDIN.tty? && ENV['TERM'])) && command_exists?('tput')
        @terminal_size = [`tput cols`.to_i, `tput lines`.to_i]
      elsif STDIN.tty? && command_exists?('stty')
        @terminal_size = `stty size`.scan(/\d+/).map { |s| s.to_i }.reverse
      else
        @terminal_size = [80, 30]
      end
    end
    
    return @terminal_size
  rescue => e
    [80, 30]
  end
  
  def get_constant_symbol(value)
    unless @constant_map
      @constant_map = {}
      
      Object.constants.each{
        |symbol|
        @constant_map[Object.const_get(symbol)] = symbol
      }
    end
    
    @constant_map[value]
  end
end

def ssh_result(command, host, user, return_object = false)
  if host == DEFAULTS
    debug("Unable to run '#{command}' because '#{host}' is not valid")
    raise RemoteCommandError.new(user, host, command, nil, '')
  end
  
  if return_object == false && 
      Configurator.instance.is_localhost?(host) && 
      user == Configurator.instance.whoami()
    return cmd_result(command)
  end

  Configurator.instance.debug("Execute `#{command}` on #{host}")
  result = ""

  ssh = Net::SSH.start(host, user, Configurator.instance.options.ssh_options)

  if return_object
    ssh.exec!(". /etc/profile; export LANG=en_US; #{command} --stream") do
      |ch, stream, data|

      unless data =~ /RemoteResult/ || result != ""
        puts data
      else
        result += data
      end
    end
  else
    result = ssh.exec!(". /etc/profile; export LANG=en_US; #{command}").to_s.chomp
  end

  rc = ssh.exec!("echo $?").chomp.to_i
  ssh.close()

  if rc != 0
    raise RemoteCommandError.new(user, host, command, rc, result)
  else
    Configurator.instance.debug("RC: #{rc}, Result: #{result}")
  end

  return result
end

def cmd_result(command, ignore_fail = false)
  Configurator.instance.debug("Execute `#{command}`")
  result = `export LANG=en_US; #{command} 2>&1`.chomp
  rc = $?
  
  if rc != 0 && ! ignore_fail
    raise CommandError.new(command, rc, result)
  else
    Configurator.instance.debug("RC: #{rc}, Result: #{result}")
  end
  
  return result
end

def output_usage_line(argument, msg = "", default = nil, max_line = nil, additional_help = "")
  if max_line == nil
    max_line = Configurator.instance.detect_terminal_size[0]-5
  end
  
  if msg.is_a?(String)
    msg = msg.split("\n").join(" ")
  else
    msg = msg.to_s()
  end
  
  msg = msg.gsub(/^\s+/, "").gsub(/\s+$/, $/)
  
  if default.to_s() != ""
    if msg != ""
      msg += " "
    end
    
    msg += "[#{default}]"
  end
  
  if argument.length > 28 || (argument.length + msg.length > max_line)
    puts argument
    
    words = msg.split(' ')
    
    force_add_word = true
    line = format("%-29s", " ")
    while words.length() > 0
      if !force_add_word && line.length() + words[0].length() > max_line
        puts line
        line = format("%-29s", " ")
        force_add_word = true
      else
        line += " " + words.shift()
        force_add_word = false
      end
    end
    puts line
  else
    puts format("%-29s", argument) + " " + msg
  end
  
  if additional_help.to_s != ""
    additional_help = additional_help.split("\n").map!{
      |line|
      line.strip()
    }.join(' ')
    additional_help.split("<br>").each{
      |line|
      output_usage_line("", line, nil, max_line)
    }
  end
end

def fill_ports_near_hosts(host_list, port_to_add)
  initial_hosts = nil
  host_list.split(",").each { |host|
    host_addr = host.strip + "[" + port_to_add + "]"
    if initial_hosts
      initial_hosts = initial_hosts + "," + host_addr
    else
      initial_hosts = host_addr
    end
  }
  return initial_hosts
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
