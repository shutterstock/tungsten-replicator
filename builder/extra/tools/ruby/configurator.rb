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

# These aren't required, but it makes the output update more often with SSH results
begin
  require 'rubygems'
  require 'net/ssh'
rescue LoadError
end

system_require 'transformer'
system_require 'validator'
system_require 'properties'
system_require 'configure/is_tools_package'
system_require 'configure/parameter_names'
system_require 'configure/configure_messages'
system_require 'configure/configure_module'
system_require 'configure/configure_package'
system_require 'configure/configure_prompt_handler'
system_require 'configure/configure_prompt'
system_require 'configure/configure_validation_handler'
system_require 'configure/configure_validation_check'
system_require 'configure/configure_deployment_handler'
system_require 'configure/configure_deployment'

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

# Manages top-level configuration.
class Configurator
  include Singleton
  
  attr_reader :modules, :deployments
  
  TUNGSTEN_COMMUNITY = "Community"
  TUNGSTEN_ENTERPRISE = "Enterprise"
  
  CLUSTER_CONFIG = "cluster.cfg"
  HOST_CONFIG = "tungsten.cfg"
  TEMP_DEPLOY_DIRECTORY = "tungsten_configure"
  TEMP_DEPLOY_HOST_CONFIG = ".tungsten.cfg"
  TEMP_DEPLOY_CLUSTER_CONFIG = "cluster.cfg"
  
  SERVICE_CONFIG_PREFIX = "service_"
  
  # Initialize configuration arguments.
  def initialize()
    # Set instance variables.
    @arguments = ARGV
    @stdin = STDIN
    @modules = []
    @deployments = []
    @remote_messages = []
    
    # This is the configuration object that will be stored
    @stored_config = Properties.new

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.output_threshold = Logger::INFO
    @options.force = false
    @options.interactive = true
    @options.advanced = false
    @options.stream_output = false
    @options.package = "ConfigurePackageCluster"

    # Check for the tungsten.cfg in the unified configs directory
    if File.exist?("#{ENV['HOME']}/configs/#{CLUSTER_CONFIG}")
      @options.config = "#{ENV['HOME']}/configs/#{CLUSTER_CONFIG}"
    else
      @options.config = "#{get_base_path()}/#{CLUSTER_CONFIG}"
    end
    
    if is_full_tungsten_package?() && !File.exist?(@options.config)
      if File.exist?("#{ENV['HOME']}/configs/#{HOST_CONFIG}")
        @options.config = "#{ENV['HOME']}/configs/#{HOST_CONFIG}"
      elsif File.exist?("#{get_base_path()}/#{HOST_CONFIG}")
        @options.config = "#{get_base_path()}/#{HOST_CONFIG}"
      end
    end
    
    Dir[File.dirname(__FILE__) + '/configure/packages/*.rb'].each do |file| 
      require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    unless parsed_options? && arguments_valid?
      output_usage()
      exit
    end
    
    # Load the current configuration values
    if File.exist?(@options.config)
      @stored_config.load(@options.config)
    end
  end

  # The standard process, collect prompt values, validate on each host
  # then deploy on each host
  def run
    unless use_streaming_ssh()
      #warning("It is recommended that you install the net-ssh rubygem")
    end
    
    write_header "Tungsten #{tungsten_version()} Configuration Procedure"
    display_help()
    
    # Locate and initialize the configure modules
    initialize_modules()
    initialize_deployments()
    
    prompt_handler = ConfigurePromptHandler.new(@stored_config)
    
    # If not running in batch mode, collect responses for configuration prompts
    if @options.interactive
      # Collect responses to the configuration prompts
      begin
        prompt_handler.run()
        save_prompts()
      rescue ConfigureSaveConfigAndExit => csce 
        write "Saving configuration values and exiting"
        save_prompts()
        exit 0
      end
    else
      # Disabled until the prompt handler is updated to collect information
      # for multiple services
      
      # Validate the values in the configuration file against the prompt validation
      #unless prompt_handler.is_valid?()
      #  write_header("There are errors with the values provided in the configuration file", Logger::ERROR)
      #  prompt_handler.print_errors()
      #  exit 1
      #end
    end
    
    deployment_method = get_deployment()
    deployment_method.set_config(@stored_config)
    unless deployment_method.validate()
      # If there were errors, collect new responses to the prompts
      if @options.interactive
        write_header("The configuration values do not pass all validation checks", Logger::ERROR)
      else
        write_header("The configuration file does not pass all validation checks", Logger::ERROR)
      end
      deployment_method.get_validation_handler().output_errors()
        
      exit 1
    end
    
    unless deployment_method.deploy()
      write_header("The deployment failed", Logger::ERROR)
      deployment_method.get_deployment_handler().errors.each{
        |error|
        error(error.message)
      }
      
      exit 1
    end
    
    info("")
    info("Deployment finished")
  end
  
  # Handle the remote side of the validation_handler->run function
  def validate
    # Locate and initialize the configure modules
    initialize_modules()
    initialize_deployments()
    
    if has_tty?() && @options.stream_output == false
      info("")
      write_header "Validation checks for #{@stored_config.getProperty(GLOBAL_HOST)}:#{@stored_config.getProperty(GLOBAL_HOME_DIRECTORY)}"
    end
    
    deployment_method = get_deployment()
    result = deployment_method.validate_config(@stored_config)
    if Configurator.instance.has_tty?() && @options.stream_output == false
      result.output()
    else
      result.messages = @remote_messages
      
      # Remove the config object so that the dump/load process is faster
      @stored_config = nil
      puts Marshal.dump(result)
    end
  end
  
  # Handle the remote side of the deployment_handler->run function
  def deploy
    # Locate and initialize the configure modules
    initialize_modules()
    initialize_deployments()
    
    if has_tty?() && @options.stream_output == false
      info("")
      write_header "Deploy #{@stored_config.getProperty(GLOBAL_HOST)}:#{@stored_config.getProperty(GLOBAL_HOME_DIRECTORY)}"
    end
    
    deployment_method = get_deployment()
    result = deployment_method.deploy_config(@stored_config)
    if has_tty?() && @options.stream_output == false
      result.output()
    else
      result.messages = @remote_messages
      
      # Remove the config object so that the dump/load process is faster
      @stored_config = nil
      puts Marshal.dump(result)
    end
  end
  
  # Locate and initialize modules in the configure/modules directory
  def initialize_modules
    Dir[File.dirname(__FILE__) + '/configure/modules/*.rb'].each do |file| 
      require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    @modules = []
    package = Module.const_get(@options.package).new()
    ConfigureModule.subclasses().each {
      |module_class|
      module_obj = module_class.new()
      module_obj.set_config(@stored_config)
      
      if module_obj.include_module_for_package?(package)
        @modules.push(module_obj)
      end
    }
    @modules = @modules.sort{|a,b| a.get_weight() <=> b.get_weight()}
  end
  
  # Locate and initialize deployments in the configure/deployments directory
  def initialize_deployments
    Dir[File.dirname(__FILE__) + '/configure/deployments/*.rb'].each do |file| 
      require File.dirname(file) + '/' + File.basename(file, File.extname(file))
    end
    
    @deployments = []
    package = Module.const_get(@options.package).new()
    ConfigureDeployment.subclasses().each {
      |deployment_class|
      deployment_obj = deployment_class.new()
      deployment_obj.set_config(@stored_config)
      
      if deployment_obj.include_deployment_for_package?(package)
        @deployments.push(deployment_obj)
      end
    }
    @deployments = @deployments.sort{|a,b| a.get_weight <=> b.get_weight}
  end
  
  def get_deployment()
    Configurator.instance.deployments.each{
      |deployment|
      if deployment.get_name() == @stored_config.getProperty(GLOBAL_DEPLOYMENT_TYPE)
        return deployment
      end
    }
    
    raise "Unable to find a matching deployment method"
  end
  
  def advanced_mode?
    @options.advanced == true
  end
  
  # Parse command line arguments.
  def parsed_options?
    opts=OptionParser.new
    opts.on("-a", "--advanced")       {|val| @options.advanced = true}
    opts.on("-b", "--batch")          {|val| @options.interactive = false}
    opts.on("-c", "--config String")  {|val| @options.config = val }
    opts.on("-f", "--force")          {|val| @options.force = true }
    opts.on("-p", "--package String")        {|val| @options.package = val }
    opts.on("-h", "--help")           {|val| output_help; exit 0}
    opts.on("-q", "--quiet")          {@options.output_threshold = Logger::ERROR}
    opts.on("-v", "--verbose")        {@options.output_threshold = Logger::DEBUG}
    opts.on("--stream")               {@options.stream_output = true }

    begin
      opts.parse!(@arguments)
    rescue
      error("Argument parsing failed")
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
      if ! File.readable?(@options.config)
        write "Config file does not exist or is not readable: #{@options.config}", Logger::ERROR
        return false
      end
    end
    
    unless defined?(@options.package)
      error("Unable to find the #{@options.package} package")
      return false
    end
    
    true
  end
  
  def save_prompts
    if !File.exists?(@options.config)
      @options.config = get_deployment().get_default_config_filename()
    end
    
    @stored_config.store(@options.config)
  end

  def output_help
    output_version
    output_usage
  end

  def output_usage
    puts "Usage: configure [options]"
    puts "-h, --help         Displays help message"
    puts "-a, --advanced     Enable advanced options"
    puts "-b, --batch        Batch execution from existing config file"
    puts "-c, --config file  Sets name of config file (default: tungsten.cfg)"
    puts "-f, --force        Skip validation checks"
    puts "-p, --package      Class name for the configuration package"
    puts "-q, --quiet        Quiet output"
    puts "-v, --verbose      Verbose output"
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
  
  def write(content="", level=Logger::INFO, hostname = nil)
    unless enable_log_level(level)
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
      hostname = @stored_config.getProperty(GLOBAL_HOST)
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
    false
    
    #if defined?(Net::SSH)
    #  true
    #else
    #  false
    #end
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