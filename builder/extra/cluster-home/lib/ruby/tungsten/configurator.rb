#!/usr/bin/env ruby
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

# System libraries.
require 'tungsten/system_require'

system_require 'optparse'
system_require 'ostruct'
system_require 'date'
system_require 'fileutils'
system_require 'socket'

# Tungsten local libraries.
require 'tungsten/parameter_names'
require 'tungsten/properties'
require 'tungsten/transformer'
require 'tungsten/subconfigurator_factory'
require 'tungsten/validator'

# Manages top-level configuration.
class Configurator
  attr_reader :options, :config

  # Global parameter names
  include ParameterNames

  #
  # Setting this option to true will cause us to
  # configure the manager to start an internal
  # monitor and will also eliminate the
  # inclusion of the monitor in the startall
  # stopall etc. scripts. In the case where a
  # monitor is needed, the monitor still gets configured
  # normally.
  #
  OPTION_MONITOR_INTERNAL = "true"

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

  TUNGSTEN_COMMUNITY = "Community"
  TUNGSTEN_ENTERPRISE = "Enterprise"
  # Initialize configuration arguments.
  def initialize(arguments, stdin)
    # Set instance variables.
    @arguments = arguments
    @stdin = stdin
    @config = nil
    @OS = nil
    @can_install_services_on_os = false
    @do_dataservice_configuration = false

    # FIXME: tungsten version (enterprise or community) is based on a
    # (too?) simple directory existance test
    if File.exist?("tungsten-replicator/lib/tungsten-enterprise-replicator.jar")
      @tungsten_version = TUNGSTEN_ENTERPRISE
    else
      @tungsten_version = TUNGSTEN_COMMUNITY
    end

    # Set command line argument defaults.
    @options = OpenStruct.new
    @options.verbose = false
    @options.manager = false

    @options.interactive = true
    @options.noapply = false

    # Check for the tungsten.cfg in the unified configs directory
    if File.exist?("#{ENV['HOME']}/configs/tungsten.cfg")
      @options.config = "#{ENV['HOME']}/configs/tungsten.cfg"
    else
      @options.config = "tungsten.cfg"
    end

    @options.force = false

    # If this is enabled, additional questions are asked.
    @options.isExpert = false

    # Define derived variables.
    @config = nil
    @OS = nil
    @can_install_services_on_os = false
    @subconfig_factory = SubConfiguratorFactory.new(self)
    @services = []
    @repl_config_helper = nil
  end

  # Parse options, check arguments, then process the command
  def run
    write_header "Tungsten #{@tungsten_version} Configuration Procedure"
    puts "NOTE:  To terminate configuration press ^C followed by ENTER"

    if parsed_options? && arguments_valid?

      puts "Start at #{DateTime.now}\n\n" if @options.verbose

      output_options if @options.verbose # [Optional]

      # Validate the environment.
      validate_environment

      # Load existing configuration file if any.
      load_config

      init_expert_properties

      # Perform editing of configuration if we are interactive.
      if @options.interactive then
        puts
        write_header "Start interactive configuration"
        puts
        puts <<INTRO
Tungsten interactive configuration will now guide you through a set
of questions to gather parameters required to set up Tungsten services. 
INTRO

        edit_config
      end

      # Perform configuration and service startup provided user wants
      # to apply them now.
      if ! @options.noapply
        apply_preconfig
        apply_config
        apply_postconfig
        print_usage_notes
      end

      puts "\nFinished at #{DateTime.now}" if @options.verbose
    else
      output_usage
    end
  end

  # Parse command line arguments.
  def parsed_options?

    opts=OptionParser.new
    opts.on("-b", "--batch")          {|val| @options.interactive = false}
    opts.on("-c", "--config String")  {|val| @options.config = val }
    opts.on("-e", "--expert")         {|val| @options.isExpert = true }
    opts.on("-f", "--force")          {|val| @options.force = true }
    opts.on("-h", "--help")           {|val| output_help; exit 0}
    opts.on("-m", "--manager")        {|val| @options.manager = true}
    opts.on("-V", "--verbose")        {@options.verbose = true}

    opts.parse!(@arguments) rescue
    begin
      puts "Argument parsing failed"
      return false
    end

    true
  end

  # Determine the operating system and validate the environment.
  def validate_environment
    puts
    write_header "Validating environment"
    if @options.force
      puts "Forced configuration; skipping validation checks"
      return
    end

    os = `uname -s`.chomp
    puts "Operating system: #{os}\n" if @options.verbose
    @options.OS = case
    when os == "Linux"then OS_LINUX
    when os == "Darwin"then OS_MACOSX
    when os == "SunOS"then OS_SOLARIS
    else OS_UNKNOWN
    end

    # Architecture is unknown by default.
    arch = `uname -m`.chomp
    @options.arch = case
    when arch == "x86_64" then OS_ARCH_64
    when arch == "i386" then OS_ARCH_32
    when arch == "i686" then OS_ARCH_32
    else
      OS_ARCH_UNKNOWN
    end

    # If the OS is unknown accept it only if this is a forced configuration.
    case @options.OS
    when OS_UNKNOWN
      validation_failure "Operating system could not be determined"
    when OS_LINUX
      puts "Checking Linux distribution"
      if File.exist?("/etc/redhat-release")
        @options.distro = OS_DISTRO_REDHAT
        @options.can_install_services_on_os = true
      elsif File.exist?("/etc/debian_version")
        @options.distro = OS_DISTRO_DEBIAN
        @options.can_install_services_on_os = true
      else
        @options.distro = OS_DISTRO_UNKNOWN
      end
    end

    # Look for Java.
    java_binary = false
    java_out = `java -version 2>&1`
    if java_out =~ /Java|JDK/
      java_binary = true
      puts "Found executable Java binary"
    else
      puts "Java binary not found in environment; checking for JAVA_HOME instead"
    end

    # Look for JAVA_HOME.  Signal if this cannot be
    if ENV['JAVA_HOME']
      if File.exist?(ENV['JAVA_HOME'])
        puts "Found JAVA_HOME directory"
      else
        if ! java_binary
          validation_failure("Cannot find supported Java binary in execution path or JAVA_HOME")
        end
      end
    end

    puts "Validation successful"
  end

  # Exit after a failed validation.
  def validation_failure(message)
    printf "Validation failure: %s\n", message
    printf "Use the -f or --force to override validation\n"
    exit 1
  end

  def output_options
    puts "Options:\n"

    @options.marshal_dump.each do |name, val|
      puts "  #{name} = #{val}"
    end
  end

  # True if required arguments were provided
  def arguments_valid?
    if @options.interactive
      # For interactive mode, must be able to write the config file.
      if File.exist?(@options.config)
        if ! File.writable?(@options.config)
          puts "Config file must be writable for interactive mode: #{@options.config}"
          return false
        end
      else
        if ! File.writable?(File.dirname(@options.config))
          puts "Config file directory must be writable for interactive mode: #{@options.config}"
          return false
        end
      end
    else
      # For batch mode, options file must be readable.
      if ! File.readable?(@options.config)
        puts "Config file does not exist or is not readable: #{@options.config}"
        return false
      end
    end
    true
  end

  def output_help
    output_version
    output_usage
  end

  def output_usage
    puts "Usage: configure [options]"
    puts "-b, --batch        Batch execution from existing config file"
    puts "-c, --config file  Sets name of config file (default: tungsten.cfg)"
    puts "-e, --expert       Ask for additional parameters"
    puts "-f, --force        Skip validation checks"
    puts "-h, --help         Displays help message"
    puts "-m, --manager      Include Tungsten Manager (dev use only)"
    puts "-V, --verbose      Verbose output"
  end

  def output_version
    puts "#{File.basename(__FILE__)} version #{VERSION}"
  end

  # Load current configuration values.
  def load_config
    @config = Properties.new
    if File.exist?(@options.config)
      @config.load_and_initialize(@options.config, ParameterNames)

      # Set the replication config helper.  If this is not set now, we have
      # a bad configuration file.
      @repl_config_helper = @subconfig_factory.repl_configurator
      if @repl_config_helper == nil
        raise StandardError, "Configuration file appears to be invalid; missing valid DBMS type"
      end
    else
      @config.init(ParameterNames)
    end
  end

  # Allow users to edit configuration.
  def edit_config

    @config.props[GLOBAL_RESTART_DBMS] = "false"

    # Figure out whether we support PostgreSQL by looking for suitable files.
    if File.exists?("tungsten-replicator/conf/sample.postgresql-wal.properties")
      dbmsDefault = nil
      if whoami() == "postgres"
        dbmsDefault = "postgresql"
      else
        dbmsDefault = "mysql"
      end
      dbmsPropertyDesc = PropertyDescriptor.new(
      "Database type (mysql|postgresql)", PV_DBMSTYPE,
      GLOBAL_DBMS_TYPE, dbmsDefault)
    else
      dbmsPropertyDesc = PropertyDescriptor.new(
      "Database type (mysql)", PV_DBMSTYPE,
      GLOBAL_DBMS_TYPE, "mysql")
    end

    # Perform edit loop until user is satisfied with values.
    while true
      # Only need to check for additional services if manager is installed. 
      if @options.manager == true
        puts
        write_header "Select components that will be active on this host"
        puts
        puts <<SERVICES
You may choose to enable a replicator service and an optional manager
service on this host.  If you enable the manager you will need to
provide additional parameters to configure the manager properly.

SERVICES
        edit_cfg_value PropertyDescriptor.new(
          "Activate Tungsten Replicator", PV_BOOLEAN, REPL_ACTIVE, "true")
          edit_cfg_value PropertyDescriptor.new(
          "Activate Tungsten Manager", PV_BOOLEAN, MGR_ACTIVE, "false")
      else
        @config.props[REPL_ACTIVE] = "true"
        @config.props[MGR_ACTIVE] = "false"
      end

      # Adding manager and replicator together automatically activates 
      # monitoring.
      if @config.props[REPL_ACTIVE] == "true" && 
          @config.props[MGR_ACTIVE] == "true" then
        config.props[REPL_MONITOR_ACTIVE] = "true"
      else
        config.props[REPL_MONITOR_ACTIVE] = "false"
      end

      if @config.props[MGR_ACTIVE] == "true"
        puts
        write_header "Specify cluster definition"
        puts
        puts <<CLUSTER_DEFINITION
If you run a managed cluster you must supply a cluster name and a list of 
DBMS host names that belong to the cluster.  The host names must resolve
to the same IP address on each host.  These values must be the same
across every node in the cluster.

CLUSTER_DEFINITION

        edit_cfg_value PropertyDescriptor.new(
          "Site name", PV_IDENTIFIER, GLOBAL_SITENAME, "default")
        edit_cfg_value PropertyDescriptor.new(
          "Cluster name", PV_IDENTIFIER, GLOBAL_CLUSTERNAME, "default")

        edit_cfg_value PropertyDescriptor.new(
          "Enter a comma-delimited list of cluster member hosts", PV_HOSTNAME,
          GLOBAL_HOSTS, "host_1, host_2")
      end

      puts
      write_header "Specify local data service name"
      puts
      puts <<SERVICE_DEFINITION
Each database has a local data service that owns the database and
reads master logs.  The local data service name is used to name the
Tungsten database that holds metadata for replication and helps
track transactions as they cross sites.

SERVICE_DEFINITION

      edit_cfg_value PropertyDescriptor.new(
        "Data service name", PV_IDENTIFIER, GLOBAL_DSNAME, "default")

      puts
      write_header "Specify host definition"
      puts
      puts <<HOST_DEFINITION
Each host must have a unique name.  Tungsten also needs a routable,
non-loopback IP address for networking, as well as a login to use
when running Tungsten software.  Non-root accounts are recommended
but you may also use root if desired.

HOST_DEFINITION
      edit_cfg_value PropertyDescriptor.new(
      "Name of this host", PV_HOSTNAME, GLOBAL_HOST, `hostname`.chomp)
      edit_cfg_value PropertyDescriptor.new(
      "IP address to use for this host", PV_HOSTNAME, GLOBAL_IP_ADDRESS,
      IPSocket::getaddress(@config.props[GLOBAL_HOST]).to_s)

      edit_cfg_value PropertyDescriptor.new(
      "Name of user to run Tungsten software", PV_IDENTIFIER, GLOBAL_USERID,
      whoami())

      puts
      write_header "Enter process configuration settings"
      puts
      puts <<SVC_INFO
The configure script can start Tungsten services automatically after
configuration completes and optionally install them to start at
boot time.  If you are running Tungsten using a non-root account
and have enabled sudo, enter sudo as the prefix for root commands.

SVC_INFO
      edit_cfg_value PropertyDescriptor.new(
      "Install service start scripts", PV_BOOLEAN, GLOBAL_SVC_INSTALL,
      "false")
      if @config.props[GLOBAL_DBMS_TYPE] == "postgresql"
        svc_start_msg = "Restart PostgreSQL server and start services after configuration"
      else
        svc_start_msg = "Start services after configuration"
      end
      edit_cfg_value PropertyDescriptor.new(
        svc_start_msg, PV_BOOLEAN, GLOBAL_SVC_START, "true")
      edit_cfg_value PropertyDescriptor.new(
        "Prefix for issuing root commands (typically 'sudo')", 
        PV_ANY, GLOBAL_ROOT_PREFIX, "")

      # TUC-192: don't ask for gc-membership method anymore, default to ping
      # but leave the logic in.
      @config.props[GLOBAL_GC_MEMBERSHIP] = "ping"

      if @config.props[GLOBAL_GC_MEMBERSHIP] == "gossip" then
        edit_cfg_value PropertyDescriptor.new(
          "Port to use for gossip server", PV_INTEGER, GLOBAL_GOSSIP_PORT,
          "12001")
        edit_cfg_value PropertyDescriptor.new(
          "Comma separated list of gossip host names", PV_HOSTNAME,
          GLOBAL_GOSSIP_HOSTS, "host1,host2")

        # See if we are a host where the gossip router will be active.
        if is_gossip_active
          puts "NOTE:  Gossip router will be activated on this host"
        else
          puts "NOTE:  Gossip router will NOT be activated on this host"
        end
      end

      # DBMS configuration.
      puts
      write_header "Enter configuration defaults for DBMS"
      puts

      puts <<DBMS
Tungsten configuration varies by database.  Choose one of the listed
DBMS types.  This will determine the proper configuration procedure
to use for replication as well as connectivity.

DBMS
      edit_cfg_value dbmsPropertyDesc

      # Create subconfigurator for this DB type and run early prechecks.
      @repl_config_helper = @subconfig_factory.repl_configurator
      @repl_config_helper.pre_checks

      # Replicator configuration.
      if @config.props[REPL_ACTIVE] == "true" then
        puts
        write_header "Enter default configuration parameters for replication services"

        # Make sure we have the right replication sub-configurator for our
        # DBMS type and then invoke edit on properties.
        @repl_config_helper.edit

        @config.props[REPL_MONITOR_INTERVAL] = "3000"
      end

      if @config.props[GLOBAL_DBMS_TYPE] == "mysql" && 
         @config.props[REPL_USE_BYTES == "false"]
        # Configure MySQL Connector/J (TENT-222).
        puts
        write_divider
        puts <<CONNECTOR_J
If you are replicating to MySQL 4.1 or below, you need to use the
MySQL Connector/J Driver, which is distributed by Oracle.  We
recommend using version 5.1.13 or above, which is available at the
following location:

   http://www.mysql.com/downloads/connector/j/

CONNECTOR_J
        edit_cfg_value PropertyDescriptor.new(
          "Use MySQL Connector/J driver?", PV_BOOLEAN, 
          GLOBAL_USE_MYSQL_CONNECTOR, "false")
        if @config.props[GLOBAL_USE_MYSQL_CONNECTOR] 
          edit_cfg_value PropertyDescriptor.new(
            "Path to MySQL Connector/J", PV_READABLE_FILE, 
            GLOBAL_MYSQL_CONNECTOR_PATH,
            "/opt/mysql/connectorJ/mysql-connector-java-5.1.13-bin.jar")
        end
      end

      @config.props[ROUTER_WAITFOR_DISCONNECT] = "5"

      puts
      puts <<STORE
Tungsten has all values needed to configure itself properly.  You
may now accept and store the values (Y), take another loop through
the list of questions (N) or quit the configuration (Q).
STORE
      if (confirmed("Accept the current values and store to file?", "")) then
        break
      end

    end # while

    # Store values.
    puts "Storing values..."
    @config.store(@options.config)

    # See if user wants to go ahead and apply.  Users who are upgradging
    # will typically not do so.
    puts
    write_divider
    puts <<APPLY
Your configuration settings have been saved in #{@options.config}.
If you are upgrading you may wish to complete configuration later
when you are ready to switch to this Tungsten version using the
following command:

  ./configure -b -c #{@options.config}

At this point you can complete configuration (Y), stop now (N), or quit
(Q).  
APPLY
    if (! confirmed("Apply configuration settings and start services now?", "")) then
      @options.noapply = true
    end
  end

  def halt_confirm()
    puts "Press ENTER to continue or ^C followed by ENTER to exit..."
    STDIN.gets
  end

  def confirmed(message, default)
    # Quit loop if values are OK.
    prompt = "#{message} [(y)es/(n)o/(q)uit]:"
    yes_set = /yes|YES|y|Y/
    no_set = /no|NO|n|N/
    quit_set = /q|Q|quit|QUIT/
    puts
    answer = read_value prompt, default
    while ((answer =~ yes_set) == nil &&
      (answer =~ no_set) == nil &&
      (answer =~ quit_set) == nil) do
      answer = read_value prompt, default
    end
    if answer =~ yes_set then
      return true
    end
    if answer =~ no_set then
      return false
    end
    puts "Exiting..."
    exit(1)
  end

  # Preform preconfiguration checks.
  def apply_preconfig
    puts
    write_header "Performing pre-configuration checks..."
    # Replicator enabled? Perform DBMS specific checks.
    if @config.props[REPL_ACTIVE] =~ /true/ then
      @repl_config_helper.pre_configure
    end
  end

  # Apply the configuration.
  def apply_config
    puts
    write_header "Configuring components..."
    apply_config_cluster_home
    apply_config_replicator
    #apply_config_sql_router
    #apply_config_connector
    apply_config_manager
    apply_config_monitor
    apply_config_bristlecone
    apply_config_services
  end

  # Configure the Tungsten Replicator.
  def apply_config_replicator
    puts
    if @config.props[REPL_ACTIVE] =~ /false/ then
      puts "Tungsten Replicator is not active; skipping configuration"
      # Just create a valid extension directory
      svc_properties_dir = "cluster-home/conf/cluster/" + @config.props[GLOBAL_DSNAME] + "/extension/"
      `mkdir -p #{svc_properties_dir}`
      return
    end

    puts "Performing replicator configuration..."

    # This first step selects the correct properties file template and
    # does DBMS-specific customization.
    @repl_config_helper.configure

    # Fix up the wrapper.conf file to allocate the right amount of memory.
    if ! @config.props[REPL_JAVA_MEM_SIZE]
      @config.props[REPL_JAVA_MEM_SIZE] = 512
    end
    transformer = Transformer.new(
    "tungsten-replicator/conf/wrapper.conf",
    "tungsten-replicator/conf/wrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.java.maxmemory=/
        "wrapper.java.maxmemory=" + @config.props[REPL_JAVA_MEM_SIZE]
      else
        line
      end
    }

    # Configure user name in replicator service script.
    set_run_as_user "tungsten-replicator/bin/replicator"

    # Add replicator service.
    add_service("tungsten-replicator/bin/replicator")
    FileUtils.cp("tungsten-replicator/conf/replicator.service.properties", "cluster-home/conf/cluster/" +
    @config.props[GLOBAL_CLUSTERNAME] +
    "/service/replicator.properties")
  end

  def apply_config_manager
    if @config.props[MGR_ACTIVE] =~ /false/
      puts "Tungsten Manager is not active on this host"
      puts "Skipping configuration"
      return
    end

    if @config.props[REPL_ACTIVE] =~ /false/ && @config.props[SQLR_USENEWPROTOCOL] == "true" then
      puts "Tungsten Manager is not required on this host because"
      puts "the replicator is not enabled and the router is using the new protocol."
      puts "Skipping configuration"
      return
    end
    puts
    puts "Performing Tungsten Manager configuration..."
    transformer = Transformer.new(
    "tungsten-manager/conf/sample.manager.properties",
    "tungsten-manager/conf/manager.properties", "# ")

    transformer.transform { |line|
      if line =~ /manager.global.site/ then
        "manager.global.site=" + @config.props[GLOBAL_SITENAME]
      elsif line =~ /manager.global.service/ then
        "manager.global.service=" + @config.props[GLOBAL_DSNAME]
      elsif line =~ /manager.global.cluster/ then
        "manager.global.cluster=" + @config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /manager.global.member/ then
        "manager.global.member=" + @config.props[GLOBAL_HOST]
      elsif line =~ /manager.gc.default_join/ then
        "manager.gc.default_join=true"
      elsif line =~ /manager.gc.group/ then
        "manager.gc.group=" + @config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /manager.gc.member/ then
        "manager.gc.member=" + @config.props[GLOBAL_HOST]
      elsif line =~ /manager.cluster.policy/ then
        "manager.cluster.policy=com.continuent.tungsten.cluster.manager.policy.EnterprisePolicyManager"
      elsif line =~ /manager.policy.mode/ then
        "manager.policy.mode=" + @config.props[POLICY_MGR_MODE]
      elsif line =~ /manager.monitor.start/ then
        if @config.props[REPL_MONITOR_ACTIVE] =~ /true/ &&
        OPTION_MONITOR_INTERNAL =~ /true/ then
          "manager.monitor.start=true"
        else
          "manager.monitor.start=false"
        end
      elsif line =~ /manager.replicator.proxy/ then
        # Hack to get correct JMX interface handler.  Should be in
        # subconfigurator.
        "manager.replicator.proxy=com.continuent.tungsten.manager.resource.proxy.ReplicatorManagerProxyImplV2"
      else
        line
      end
    }

    # Setup group discovery via TCPPING.
    if @config.props[GLOBAL_GC_MEMBERSHIP] == "ping" then
      # Construct a host list for port numbers on each host.
      ping_port = "7800"
      initial_hosts = fill_ports_near_hosts(@config.props[GLOBAL_HOSTS], ping_port)
      # Fill config files.
      ping_xml = "/jgroups_tcp_ping.xml"
      apply_config_hedera("tungsten-manager/conf/hedera.properties", ping_xml)
      apply_config_ping_jgroups("tungsten-manager/conf" + ping_xml, initial_hosts)
    end

    # Setup group discovery via MULTICAST.
    if @config.props[GLOBAL_GC_MEMBERSHIP] == "multicast" then
      # Fill config files.
      tcp_xml = "/jgroups_tcp.xml"
      apply_config_hedera("tungsten-manager/conf/hedera.properties", tcp_xml)
      apply_config_multicast_jgroups("tungsten-manager/conf" + tcp_xml)
    end

    # Configure user name in service script.
    set_run_as_user "tungsten-manager/bin/manager"

    # Register manager service.
    add_service("tungsten-manager/bin/manager")
    FileUtils.cp("tungsten-manager/conf/manager.service.properties", "cluster-home/conf/cluster/" + @config.props[GLOBAL_CLUSTERNAME] + "/service/manager.properties")
  end

  def fill_ports_near_hosts(host_list, port_to_add)
    host_list = host_list.split(",")
    initial_hosts = nil
    host_list.each { |host|
      host_addr = host.strip + "[" + port_to_add + "]"
      if initial_hosts
        initial_hosts = initial_hosts + "," + host_addr
      else
        initial_hosts = host_addr
      end
    }
    return initial_hosts
  end

  def apply_config_hedera(hedera_properties, jgroups_xml)
    transformer = Transformer.new(
    hedera_properties,
    hedera_properties, nil)
    transformer.transform { |line|
      if line =~ /hedera.channel.jgroups.config/ then
        "hedera.channel.jgroups.config=" + jgroups_xml
      else
        line
      end
    }
  end

  def apply_config_ping_jgroups(jgroups_xml, initial_hosts)
    transformer = Transformer.new(jgroups_xml, jgroups_xml, nil)
    transformer.transform { |line|
      if line =~ /<TCPPING initial_hosts/ then
        "<TCPPING initial_hosts=\"" + initial_hosts + "\""
      elsif line =~ /<TCP bind_addr="([0-9.]*)"/ then
        line.sub $1, @config.props[GLOBAL_IP_ADDRESS]
      else
        line
      end
    }
  end

  def apply_config_multicast_jgroups(jgroups_xml)
    transformer = Transformer.new(jgroups_xml, jgroups_xml, nil)
    transformer.transform { |line|
      if line =~ /<TCP bind_addr="([0-9.]*)"/ then
        line.sub $1, @config.props[GLOBAL_IP_ADDRESS]
      else
        line
      end
    }
  end

  # Perform generic monitor configuration.
  def apply_config_monitor
    puts
    if @config.props[REPL_MONITOR_ACTIVE] =~ /false/ then
      puts "Monitor is not active; skipping configuration"
      return
    end

    puts "Performing Tungsten Monitor configuration..."
    transformer = Transformer.new(
    "tungsten-monitor/conf/sample.monitor.properties",
    "tungsten-monitor/conf/monitor.properties", "# ")

    transformer.transform { |line|
      if line =~ /check.frequency.ms/
        "check.frequency.ms=" + @config.props[REPL_MONITOR_INTERVAL]
      elsif line =~ /cluster.name/
        "cluster.name=" + @config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /cluster.member/
        "cluster.member=" + @config.props[GLOBAL_HOST]
      elsif line =~ /notifier.gcnotifier.channelName=/
        "notifier.gcnotifier.channelName=" + @config.props[GLOBAL_CLUSTERNAME] + ".monitoring"
      else
        line
      end
    }

    # Decide which replicator checker to use based on replication manager.
    checkerfile = "checker.tungstenreplicator.properties"

    # Configure monitoring for regular Tungsten Replicator.
    transformer = Transformer.new(
    "tungsten-monitor/conf/sample.#{checkerfile}",
    "tungsten-monitor/conf/#{checkerfile}", "# ")

    transformer.transform { |line|
      if line =~ /frequency=/
        "frequency=" + @config.props[REPL_MONITOR_INTERVAL]
      elsif line =~ /sourceId=/
        "sourceId=" + @config.props[GLOBAL_HOST]
      elsif line =~ /clusterName=/
        "clusterName=" + @config.props[GLOBAL_CLUSTERNAME]
      else
        line
      end
    }

    # Configure user name in service script.
    set_run_as_user "tungsten-monitor/bin/monitor"

    # Register the monitor service.
    if OPTION_MONITOR_INTERNAL =~ /false/ then
      add_service("tungsten-monitor/bin/monitor")
    end
    FileUtils.cp("tungsten-monitor/conf/monitor.service.properties", "cluster-home/conf/cluster/" + @config.props[GLOBAL_CLUSTERNAME] + "/service/monitor.properties")
  end

  def apply_config_hedera_monitoring(hedera_monitoring_properties)
    transformer = Transformer.new(
    hedera_monitoring_properties,
    hedera_monitoring_properties, nil)

    transformer.transform { |line|
      if line =~ /hedera.channel.name=/
        "hedera.channel.name=" + @config.props[GLOBAL_CLUSTERNAME] + ".monitoring"
      else
        line
      end
    }
  end

  # Update configuration for bristlecone.
  def apply_config_bristlecone
    puts
    puts "Performing Bristlecone performance test configuration"

    user = @config.props[CONN_CLIENTLOGIN]
    password = @config.props[CONN_CLIENTPASSWORD]
    if password == nil then
      password = ""
    end
    siteName = @config.props[GLOBAL_SITENAME]
    clusterName = @config.props[GLOBAL_CLUSTERNAME]
    dataServiceName = @config.props[GLOBAL_DSNAME]

    transformer = Transformer.new(
    "bristlecone/config/evaluator/sample.readwrite.xml",
    "bristlecone/config/evaluator/sample.readwrite.xml", nil)

    transformer.transform { |line|
      if line =~ /user="(.*)"/ then
        line.sub $1, user
      elsif line =~ /password="(.*)"/ then
        line.sub $1, password
      elsif line =~ /url=".*(default).*"/ then
        line.sub $1, siteName + ":" + clusterName + "//" + dataServiceName
      else
        line
      end
    }

    transformer = Transformer.new(
    "bristlecone/config/evaluator/sample.readonly.xml",
    "bristlecone/config/evaluator/sample.readonly.xml", nil)

    transformer.transform { |line|
      if line =~ /user="(.*)"/ then
        line.sub $1, user
      elsif line =~ /password="(.*)"/ then
        line.sub $1, password
      elsif line =~ /url=".*(default).*"/ then
        line.sub $1, siteName + ":" + clusterName + "//" + dataServiceName
      else
        line
      end
    }
  end

  # Set up files in the cluster home.
  def apply_config_cluster_home
    if @config.props[MGR_ACTIVE] =~ /true/
      puts
      puts "Performing cluster home configuration"

      # Create data service directory.
      puts "Creating directories for services and data sources and data services"
      system "mkdir -p cluster-home/conf/cluster/" + @config.props[GLOBAL_CLUSTERNAME] + "/datasource"
      system "mkdir -p cluster-home/conf/cluster/" + @config.props[GLOBAL_CLUSTERNAME] + "/dataservice"
      system "mkdir -p cluster-home/conf/cluster/" + @config.props[GLOBAL_CLUSTERNAME] + "/service"

      if (@config.props[GLOBAL_HOSTS] != "")
        clusters = "cluster-home/conf/clusters.properties"
        out = File.open(clusters, "w")
        other_hosts = @config.props[GLOBAL_HOSTS].split(",")
        other_hosts.each { |host|
          out.puts "cluster.#{host}=9997\n"
        }
        out.close
        puts ">> GENERATED FILE: " + clusters
      end
    end
  end

  # Set up files and perform other configuration for services.
  def apply_config_services
    puts
    puts "Performing services configuration"

    # Patch for Ubuntu 64-bit start-up problem.
    if @options.distro == OS_DISTRO_DEBIAN && @options.arch == OS_ARCH_64
      wrapper_file = "cluster-home/bin/wrapper-linux-x86-32"
      if File.exist?(wrapper_file)
        FileUtils.rm("cluster-home/bin/wrapper-linux-x86-32")
      end
    end

    # Create startall script.
    script = "cluster-home/bin/startall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Start all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.each { |svc| out.puts svc + " start" }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + script

    # Create startallsvcs script.
    if @options.can_install_services_on_os
      script = "cluster-home/bin/startallsvcs"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Start all services"
      @services.each { |svc|
        svcname = File.basename svc
        if @options.distro == OS_DISTRO_REDHAT
          out.puts "/sbin/service t" + svcname + " start"
        elsif @options.distro == OS_DISTRO_DEBIAN
          out.puts "/etc/init.d/t" + svcname + " start"
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      puts ">> GENERATED FILE: " + script
    end

    # Create stopall script.
    script = "cluster-home/bin/stopall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Stop all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.reverse_each { |svc| out.puts svc + " stop" }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + script

    # Create stopallsvcs script.
    if @options.can_install_services_on_os
      script = "cluster-home/bin/stopallsvcs"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Stop all services"
      @services.reverse_each { |svc|
        svcname = File.basename svc
        if @options.distro == OS_DISTRO_REDHAT
          out.puts "/sbin/service t" + svcname + " stop"
        elsif @options.distro == OS_DISTRO_DEBIAN
          out.puts "/etc/init.d/t" + svcname + " stop"
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      puts ">> GENERATED FILE: " + script
    end

    # Create deployall script.
    if @options.can_install_services_on_os
      script = "cluster-home/bin/deployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Install services into /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        out.puts "ln -fs $PWD/" + svc + " /etc/init.d/t" + svcname
        if @options.distro == OS_DISTRO_REDHAT
          out.puts "/sbin/chkconfig --add t" + svcname
        elsif @options.distro == OS_DISTRO_DEBIAN
          out.puts "update-rc.d t" + svcname + " defaults"
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      puts ">> GENERATED FILE: " + script
    end

    # Create undeployall script.
    if @options.can_install_services_on_os
      script = "cluster-home/bin/undeployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Remove services from /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        if @options.distro == OS_DISTRO_REDHAT
          out.puts "/sbin/chkconfig --del t" + svcname
          out.puts "\\rm -f /etc/init.d/t" + svcname
        elsif @options.distro == OS_DISTRO_DEBIAN
          out.puts "\\rm -f /etc/init.d/t" + svcname
          out.puts "update-rc.d -f  t" + svcname + " remove"
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      puts ">> GENERATED FILE: " + script
    end

    # Create script to start bristlecone test with read-only connection.
    script = "cluster-home/bin/evaluator_readonly"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Run bristlecone evaluator with SQL Router readonly connection"
    out.puts "BRI_HOME=`dirname $0`/../../bristlecone"
    out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.readonly.xml $*"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + script

    # Create script to start bristlecone test with read-write connection.
    script = "cluster-home/bin/evaluator_readwrite"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Run bristlecone evaluator with SQL Router read/write connection"
    out.puts "BRI_HOME=`dirname $0`/../../bristlecone"
    out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.readwrite.xml $*"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + script

  end

  # Perform post configuration activities.
  def apply_postconfig
    puts
    write_header "Performing post-configuration activities"

    # Perform replication post-config tasks.
    if @config.props[REPL_ACTIVE] =~ /true/ then
      @repl_config_helper.post_configure
    end

    # Clean up files.
    owner = @config.props[GLOBAL_USERID]
    current_user = ENV["USER"]
    chown_command = "chown -R " + owner + " ."
    chgrp_command = "chgrp -R " + owner + " ."
    if owner != current_user
      if current_user == "root" || Process.euid == 0
        puts "Setting ownership of all files to proper owner: " + owner
        system chown_command
        system chgrp_command
      else
        puts "Unable to set file ownership; must be root"
        puts "Please cd to release directory and run the following commands as root: "
        puts chown_command
        puts chgrp_command
      end
    end

    # Install and start services.
    current_user = ENV["USER"]
    if @config.props[GLOBAL_SVC_INSTALL] == "true" then
      if @options.can_install_services_on_os
        if current_user == "root"
          puts "Deploying services by running deployall script"
          system "cluster-home/bin/deployall"
        else
          puts "Unable to set up service; must be root"
          puts "Please run the following command as root: [sudo] cluster-home/bin/deployall"
        end
      else
        puts "Service installation is not supported on this platform hence skipped"
      end
    else
      puts "Service installation skipped per user request"
    end
    if @config.props[GLOBAL_SVC_START] == "true" then
      if (@config.props[GLOBAL_RESTART_DBMS] == "defer")
        puts
        puts <<RESTART_ADVICE
WARNING: The configuration process made some changes that require that your
DBMS server be restarted on this node. You elected to defer the restart.
We cannot, therefore, start the Tungsten services at this time. You should
restart the DBMS server on this node and then use the following script to
start the Tungsten services:

RESTART_ADVICE

        if current_user == "root" && @options.can_install_services_on_os && @config.props[GLOBAL_SVC_INSTALL] == "true"
          puts "cluster-home/bin/startallsvcs"
        else
          puts "cluster-home/bin/startall"
        end

        puts ""
        puts "Service startup will be skipped at this time."

      else
        if current_user == "root" && @options.can_install_services_on_os && @config.props[GLOBAL_SVC_INSTALL] == "true"
          puts "Starting services by running startallsvcs script"
          system "cluster-home/bin/startallsvcs"
        else
          puts "Starting services by running startall script"
          system "cluster-home/bin/startall"
        end
      end
    else
      puts "Service start skipped per user request"
    end
  end

  def print_usage_notes
    puts
    write_header "Usage Notes and Advice"
    puts

    if @config.props[REPL_ACTIVE] =~ /true/ then
      puts <<REPL_ADVICE
Replicator Setup
================
You have set up Tungsten Replicator base configuration, which provides 
defaults for operation and optionally starts the replicator process.  To 
start replication you must now add one or more replication services using
the 'configure-service' script.  The following examples show how to add
master and slave services using the default service name set in this 
script. 

  ./configure-service --create --config=#{@options.config} --role=master #{@config.props[GLOBAL_DSNAME]}
  ./configure-service --create --config=#{@options.config} --role=slave #{@config.props[GLOBAL_DSNAME]}

You can get more information on service configuration using 

  ./configure-service --help --config=#{@options.config}

REPL_ADVICE

      # Print DBMS-specific replication advice.
      @repl_config_helper.print_usage
    end

    puts <<GENERIC_ADVICE
Parting Advice
==============
Configuration is now complete.  For further information, please consult
Tungsten documentation, which is available at www.continuent.com.
GENERIC_ADVICE

  end

  # Write a header
  def write_header(content)
    puts "#############################################################################"
    printf "# %s\n", content
    puts "#############################################################################"
  end

  # Write a sub-divider, which is used between sections under a singl header.
  def write_divider
    puts "-----------------------------------------------------------------------------"
  end

  # Read a value from the command line.
  def read_value(prompt, default)
    if (default.length + prompt.length < 75)
    printf "%s [%s]: ", prompt, default
    else
      printf "%s\n[%s]:", prompt, default
    end
    value = STDIN.gets
    value.strip!
    if value == ""
    return default
    else
      return value
    end
  end

  # Read and edit a configuration value.
  def edit_cfg_value(descriptor)
    if (@config.props[descriptor.key] != nil &&
    @config.props[descriptor.key] != "")
      default = @config.props[descriptor.key]
    elsif descriptor.default
      default = descriptor.default
    else
      default = ""
    end
    value = nil
    while value == nil do
      begin
        raw_value = read_value(descriptor.desc, default)
        value = descriptor.accept raw_value
      rescue PropertyValidatorException => e
        puts e.to_s
      end
    end
    @config.props[descriptor.key] = value
  end

  # Set config value to a default, i.e., only if already unset.
  def set_cfg_default(key, value)
    @config.setDefault(key, value)
  end

  # Update the RUN_AS_USER in a service script.
  def set_run_as_user(script)
    transformer = Transformer.new(script, script, nil)

    # Have to be careful to set first RUN_AS_USER= only or it
    # corrupts the start script.
    already_set = false
    transformer.transform { |line|
      if line =~ /RUN_AS_USER=/ && ! already_set then
        already_set = true
        "RUN_AS_USER=" + @config.props[GLOBAL_USERID]
      else
        line
      end
    }
  end

  # Returns true if this is the community edition of Tungsten.
  def is_community
    @tungsten_version == TUNGSTEN_COMMUNITY
  end

  # Add an OS service that needs to be started and/or deployed.
  def add_service(start_script)
    @services.insert(-1, start_script)
  end

  def get_root_prefix()
    prefix = @config.props[GLOBAL_ROOT_PREFIX]
    if prefix == "none" or prefix == "" or prefix == nil
      return ""
    else
      return prefix
    end
  end

  def get_svc_command(boot_script)
    prefix = get_root_prefix
    if prefix == ""
      return boot_script
    else
      return prefix + " " + boot_script
    end
  end

  # Generate a cluster service properties file for a system service.
  def write_svc_properties(name, boot_script)
    cluster = @config.props[GLOBAL_CLUSTERNAME]
    props_name = name + ".properties"
    svc_properties_dir = "cluster-home/conf/cluster/" + cluster + "/service"
    svc_properties = svc_properties_dir + "/" + props_name
    svc_command = get_svc_command(boot_script)

    # Ensure services properties directory exists.
    `mkdir -p #{svc_properties_dir}`

    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts "# #{props_name}"
    out.puts "name=#{name}"
    out.puts "command.start=#{svc_command} start"
    out.puts "command.stop=#{svc_command} stop"
    out.puts "command.restart=#{svc_command} restart"
    out.puts "command.status=#{svc_command} status"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + svc_properties
  end

  # Generate file that has default monitoring hooks
  def write_mon_extension_properties()
    cluster = @config.props[GLOBAL_CLUSTERNAME]
    prefix = @config.props[GLOBAL_ROOT_PREFIX]
    props_name = "event.properties"
    svc_properties_dir = "cluster-home/conf/cluster/" + cluster + "/extension"
    svc_properties = svc_properties_dir + "/" + props_name
    if prefix == "none"
      prefix_to_append = ""
    else
      prefix_to_append = prefix + " "
    end

    # Ensure services properties directory exists.
    `mkdir -p #{svc_properties_dir}`

    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts("# event.properties")
    out.puts("name=event")
    out.puts("command.onResourceStateTransition=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onDataSourceStateTransition=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onFailover=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onPolicyAction=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onRecovery=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onDataSourceCreate=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onResourceNotification=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + svc_properties
  end

  # Write a replicator monitoring file.  Supports new open replicator
  # and existing JMX replicator interfaces.
  def write_replicator_jmx_monitor_config(jmxtype)
    if jmxtype == "tungstenreplicator"
      checkerfile = "checker.tungstenreplicator.properties"
    elsif jmxtype == "openreplicator"
      checkerfile = "checker.openreplicator.properties"
    else
      raise UserError, "Unknown replicator interface type: #{jmxtype}"
    end

    # Configure monitoring for regular Tungsten Replicator.
    transformer = Transformer.new(
    "tungsten-monitor/conf/sample.#{checkerfile}",
    "tungsten-monitor/conf/#{checkerfile}", "# ")

    transformer.transform { |line|
      if line =~ /frequency=/
        "frequency=" + @config.props[REPL_MONITOR_INTERVAL]
      elsif line =~ /sourceId=/
        "sourceId=" + @config.props[GLOBAL_HOST]
      elsif line =~ /clusterName=/
        "clusterName=" + @config.props[GLOBAL_CLUSTERNAME]
      else
        line
      end
    }

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

  #################################################################
  # EXPERT CONFIGURATION OPTIONS
  #################################################################

  def init_expert_properties
    # Set default values
    if @config.props[MGR_DB_PING_TIMEOUT] == ""
      @config.props[MGR_DB_PING_TIMEOUT] = "15"
    end
    if @config.props[MGR_HOST_PING_TIMEOUT] == ""
      @config.props[MGR_HOST_PING_TIMEOUT] = "5"
    end
    if @config.props[MGR_IDLE_ROUTER_TIMEOUT] == ""
      @config.props[MGR_IDLE_ROUTER_TIMEOUT] = "3600"
    end
    if @config.props[MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS] == ""
      @config.props[MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS] = "2"
    end
    if @config.props[MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD] == ""
      @config.props[MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD] = "30"
    end
    if @config.props[MGR_NOTIFICATIONS_TIMEOUT] == ""
      @config.props[MGR_NOTIFICATIONS_TIMEOUT] = "30000"
    end
    if @config.props[MGR_NOTIFICATIONS_SEND] == ""
      @config.props[MGR_NOTIFICATIONS_SEND] = "true"
    end
    if @config.props[MON_DB_QUERY_TIMEOUT] == ""
      @config.props[MON_DB_QUERY_TIMEOUT] = "5"
    end
    if @config.props[MON_DB_CHECK_FREQUENCY] == ""
      @config.props[MON_DB_CHECK_FREQUENCY] = "3000"
    end
    if @config.props[MON_REPLICATOR_CHECK_FREQUENCY] == ""
      @config.props[MON_REPLICATOR_CHECK_FREQUENCY] = "3000"
    end
    if @config.props[REPL_THL_LOG_RETENTION] == ""
      @config.props[REPL_THL_LOG_RETENTION] = "7d"
    end
    if @config.props[REPL_CONSISTENCY_POLICY] == ""
      @config.props[REPL_CONSISTENCY_POLICY] = "stop"
    end
    if @config.props[SQLR_DELAY_BEFORE_OFFLINE] == ""
      @config.props[SQLR_DELAY_BEFORE_OFFLINE] = "600"
    end
    if @config.props[SQLR_KEEP_ALIVE_TIMEOUT] == ""
      @config.props[SQLR_KEEP_ALIVE_TIMEOUT] = "0"
    end
    if @config.props[REPL_THL_LOG_FILE_SIZE] == ""
      @config.props[REPL_THL_LOG_FILE_SIZE] = "1000000000"
    end
    if @config.props[ROUTER_WAITFOR_DISCONNECT] == ""
      @config.props[ROUTER_WAITFOR_DISCONNECT] = "5"
    end
  end
end
