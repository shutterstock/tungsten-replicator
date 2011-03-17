#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

# System libraries.
require 'tungsten/system_require'

system_require 'optparse'
system_require 'date'
system_require 'fileutils'

# Tungsten local libraries.
require 'tungsten/environment_validator'
require 'tungsten/validator'


# Manages preparation of installation directories. 
class Preparer
  attr_reader :options, :config

  # Structure to hold configuration options. 
  PrepareOptions = Struct.new(:batch, :dbms, :dbuser, :dbpassword, :fetch, 
                     :help, :prep, :reldir, :verbose)

  # Build-related parameters. 
  MANIFEST = ".manifest"
  RELEASE_REPOSITORY = "https://s3.amazonaws.com/releases.continuent.com"

  # Initialize configuration arguments.
  def initialize(arguments, stdin)
    # Set instance variables.
    @arguments = arguments
    @stdin = stdin
    @validator_factory = EnvironmentValidatorFactory.new()

    # Set basic command line argument defaults.  We fill in other defaults 
    # later when we know if we are in batch vs. interactive mode. 
    @options = PrepareOptions.new()
    @options.verbose = false
    @options.batch = false
    @options.help = false

    #@options.reldir = "/opt/tungsten"
    #@options.prep = false
    #@options.fetch = false

    # Version is nil until we find build. 
    @version = nil
  end

  # Parse options, check arguments, then process the command
  def run
    if parsed_options? && !@options.help
      puts "Start at #{DateTime.now}\n\n" if @options.verbose
      output_options if @options.verbose 

      # Fill in missing options. 
      process_intro

      # Validate the environment.
      validate_version
      validate_environment
      validate_network
      validate_dbms

      # Create/refresh release directory if desired. 
      if @options.prep
        create_release_dir
      end

      # Fetch software if desired. 
      if @options.fetch
        fetch_software
      end

      puts "\nFinished at #{DateTime.now}" if @options.verbose
    else
      output_usage
    end
  end

  # Parse command line arguments.
  def parsed_options?
    opts=OptionParser.new
    opts.on("-a", "--all")  {
      @options.prep = true
      @options.fetch = true
    }
    opts.on("-b", "--batch")              {@options.batch = true }
    opts.on("-d", "--dbms String")        {|val| @options.dbms = val }
    opts.on("-f", "--fetch")              {@options.fetch = true }
    opts.on("-h", "--help")               {@options.help = true }
    opts.on("-P", "--prep")               {@options.prep = true }
    opts.on("-p", "--dbpassword String")  {|val| @options.dbpassword = val }
    opts.on("-t", "--tungsten-home String")  {|val| @options.reldir = val } 
    opts.on("-u", "--dbuser String")      {|val| @options.dbuser = val } 
    opts.on("-V", "--verbose")            {@options.verbose = true }

    begin
      opts.parse!(@arguments) 
    rescue Exception => e
      puts "Argument parsing failed: #{e.to_s}"
      return false
    end

    true
  end

  # Write header and 
  def process_intro
    write_header "Tungsten Preparation Procedure"

    # Add in option defaults. 
    if ! @options.reldir
      @options.reldir = "/opt/tungsten"
    end
    if ! @options.prep
      @options.prep = false
    end
    if ! @options.fetch
      @options.fetch = false
    end
    if ! @options.dbms
      @options.dbms = "mysql"
    end
    if ! @options.dbuser
      @options.dbuser = "root"
    end
    if ! @options.dbpassword
      @options.dbpassword = ""
    end

    # Print an introductory message.  
    puts <<INTRO

Welcome!  You are running the Tungsten 'prepare' script, which guides you
through the installation process.  

1. Ensure prerequisites are satisfied to install Tungsten software. 
2. Create Tungsten home directory structure. 
3. Fetch and unpack the latest Tungsten build. 

You can stop this script at any time by pressing ^C followed by ENTER.  

INTRO

    # Run interactively if -b was not set. 
    if @options.batch
      puts "You are running in batch mode.  All options have already been set."
    else
      puts <<INTERACTIVE_INTRO
You are running in interactive mode.  The script will now ask you questions
to help guide checking prerequisites and setup for installation.  
INTERACTIVE_INTRO

      # Find out where Tungsten home is located.  
      puts <<HOME

#-------------------------------------------------------------------------
Where is your Tungsten home directory located?  This is the root
directory for the Tungsten release.  It must already exist and must
be writable by the current login.
HOME
      @options.reldir = edit_option("Tungsten home directory?",
        PV_WRITABLE_DIR, @options.reldir)

      # Inquire whether user would like said directory to be prepared. 
      puts <<PREP

#-------------------------------------------------------------------------
Would you like to create a Tungsten home directory structure?  If
you answer 'true' the directory structure will be created or will
be checked if it already exists.  If you answer 'false' the directory
structure will not be created or checked.  Answer 'true' if you
plan to install software.
PREP
      prep_string = edit_option(
        "Create Tungsten home directory? (true|false)", 
        PV_BOOLEAN, @options.prep.to_s)
      if prep_string == "true"
        @options.prep = true
      else
        @options.prep = false
      end

      # Inquire whether user would like to fetch software. 
      puts <<FETCH

#-------------------------------------------------------------------------
Would you like to fetch and unpack the latest Tungsten release from
Amazon S3?  Answer 'true' to fetch it.
FETCH
      fetch_string = edit_option("Fetch latest release? (true|false)", 
        PV_BOOLEAN, @options.fetch.to_s)
      if fetch_string == "true"
        @options.fetch = true
      else
        @options.fetch = false
      end

      # Find out the DBMS type. 
      puts <<DBMS

#-------------------------------------------------------------------------
Is there a database on this host and if so which one?  Answer 'mysql'
or 'postgresql' if you will have a database here.  Otherwise, answer
'none'.
DBMS
      @options.dbms = edit_option("Database type? (mysql|postgresql|none)",
        PV_DBMSTYPE, @options.dbms)

      if @options.dbms != "none"
        puts <<DBLOGIN

#-------------------------------------------------------------------------
What is the DBMS login and password that you will use for replication?
This login must already exist on the database server.
DBLOGIN
        @options.dbuser = edit_option("Database login for Tungsten", 
          PV_IDENTIFIER, @options.dbuser)
        @options.dbpassword = edit_option( "Database password for Tungsten", 
          PV_IDENTIFIER, @options.dbpassword)
      end
    end
  end

  # Validate the software version.  
  def validate_version
    puts
    write_header "Validating Build Version"

    # Read manifest to find build version. 
    begin
      File.open(MANIFEST, 'r') do |file|
        file.read.each_line do |line|
          line.strip!
          if line =~ /RELEASE:\s*(tungsten.*[0-9])/
            @version = $1
          end
        end
      end
    rescue Exception => e
      puts "Unable to read .manifest file: #{e.to_s}"
      exit 1
    end 

    # Ensure we found the version. 
    if @version
      puts "Build version: #{@version}"
    else
      puts "Unable find version in .manifest file; need a build to continue"
      exit 1
    end
  end

  # Determine the operating system and validate the environment.
  def validate_environment
    puts
    write_header "Validating environment"
    run_validators(@validator_factory.os_validators(@options))
  end

  # Perform validations of the network.  
  def validate_network
    puts
    write_header "Validating network"
    run_validators(@validator_factory.network_validators(@options))

    # uname -n should resolve to unique name of host. 
    # hostname --ip-address must resolve to real IP address, not 127.0.0.1. 
    # dbms host names must be resolvable and pingable
  end

  # Perform validations of the data source. 
  def validate_dbms
    puts
    write_header "Validating DBMS"
    run_validators(@validator_factory.mysql_validators(@options))

    # ensure mysql is in the path
    # ensure binlogs are enabled
    # ensure that the dbms is up and running
    # ensure we have an admin account with full privileges. 
    # must have ability to restart dbms e.g., 'sudo service mysqld restart'
  end

  # Run validators. 
  def run_validators validators
    # Perform validation. 
    validators.each { | validator |
      write_separator
      puts "CHECK      : #{validator.title}"
      puts "DESCRIPTION: #{validator.description}"
      begin
        validator.validate()
      rescue Exception => e
        validator.validation_failure e.to_s
      end
      puts "RESULT     : #{validator.result}"
      puts "MESSAGE    : #{validator.result_message}"
    }

    # Print summary of results including warnings. 
    write_separator
    puts "RESULTS:"
    validators.each { | validator |
      puts "#{validator.title}: #{validator.result}"
      if validator.warnings && validator.warnings.length > 0
        validator.warnings.each { | warning |
          puts "  Warning: #{warning}"
        }
      end
    }
    write_separator
  end 

  # Record a validation failure. 
  def validation_failure(message)
    printf "Validation failure: %s\n", message
  end

  # Record a validation warning. 
  def validation_warning(message)
    printf "Validation failure: %s\n", message
  end

  # Create the release directory and grab software. 
  def create_release_dir
    puts
    write_header "Creating/validating Tungsten home directory"
    
    # Ensure Tungsten home directory and child directories exist. 
    mkdir_if_absent @options.reldir
    mkdir_if_absent "#{@options.reldir}/backups"
    mkdir_if_absent "#{@options.reldir}/configs"
    mkdir_if_absent "#{@options.reldir}/logs"
    mkdir_if_absent "#{@options.reldir}/relay-logs"
    mkdir_if_absent "#{@options.reldir}/releases"
    mkdir_if_absent "#{@options.reldir}/share"

    # Create share/env.sh script. 
    Dir.chdir @options.reldir
    script = "share/env.sh"
    out = File.open(script, "w")
    out.puts "# Source this file to set your environment."
    out.puts "export TUNGSTEN_HOME=#{@options.reldir}"
    out.puts "export PATH=$TUNGSTEN_HOME/tungsten/tungsten-manager/bin:$TUNGSTEN_HOME/tungsten/tungsten-replicator/bin:$PATH"
    out.chmod(0644)
    out.close
    puts ">> GENERATED FILE: " + script
  end

  # Fetch software from web. 
  def fetch_software
    puts "Fetching software..."
    if File.exists? "#{@options.reldir}/releases"
      Dir.chdir "#{@options.reldir}/releases"
      exec_cmd2 "wget --no-check-certificate #{RELEASE_REPOSITORY}/#{@version}.tar.gz", false

      # Unpack.  
      puts "Unpacking..."
      Dir.chdir @options.reldir
      exec_cmd2 "tar -xzf releases/#{@version}.tar.gz", false

      # Put in tungsten link. 
      puts "Creating tungsten link to release..."
      exec_cmd2 "rm -f tungsten", true
      exec_cmd2 "ln -s #{@version} tungsten", true
    else
      puts "ERROR: Unable to fetch software because releases directory is missing: #{@options.reldir}/releases"
    end
  end
 
  def output_options
    puts "Options:\n"

    @options.marshal_dump.each do |name, val|
      puts "  #{name} = #{val}"
    end
  end

  # Print a nice help description. 
  def output_usage
    puts "Description: Check prerequisites for Tungsten installation"
    puts "Usage: prepare [options]"
    puts "-a, --all          Prepare and fetch (= -f + -p)"
    puts "-b, --batch        Run in non-interactive mode"
    puts "-d, --dbms         DBMS to use (mysql or postgresql, default mysql)"
    puts "-f, --fetch        Fetch latest Tungsten release"
    puts "-h, --help         Displays help message"
    puts "-p, --dbpassword   DBMS user password"
    puts "-P, --prep         Prepare Tungsten release directory"
    puts "-t, --tungsten-home dir  Sets name of the Tungsten home directory"
    puts "-u, --dbuser       DBMS user login (must already exist)"
    puts "-V, --verbose      Verbose output"
    puts
    puts "Hint: Run './prepare' without options to be guided through checks."
  end

  # Write a header
  def write_header(content)
    puts "##########################################################################"
    printf "# %s\n", content
    puts "##########################################################################"
  end

  # Write a separator. 
  def write_separator
    puts "#-------------------------------------------------------------------------"
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

  # Create a directory if it is absent. 
  def mkdir_if_absent(dirname)
    if File.exists?(dirname)
      if @options.verbose
        puts "Found directory, no need to create: #{dirname}"
      end
    else
      puts "Creating missing directory: #{dirname}"
      Dir.mkdir(dirname)
    end
  end

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

  # Read and edit an option value. 
  def edit_option(desc, validator, default)
    value = nil
    while value == nil do
      begin
        raw_value = read_value(desc, default)
        value = validator.validate raw_value
      rescue PropertyValidatorException => e
        puts e.to_s
      end
    end
    value
  end
end
