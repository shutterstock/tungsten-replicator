# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

require 'tungsten/subconfigurator'

# Base class for handling subconfiguration of Oracle replication.
class SubConfiguratorOracle  < SubConfigurator
  include ParameterNames

  # MySQL-specific property validators.
  PV_ORACLE_BACKUP_METHOD = PropertyValidator.new("none|export|script",
    "Value must be none, export, or script")

  def initialize(configurator)
    @configurator = configurator
  end

  # Early system prerequisite checks.
  def pre_checks()
    # TODO: create checks for Oracle.
  end

  # Perform pre-configuration checks.
  def pre_configure()
    # No-op for Oracle replication.
  end

  # Edit configuration items used by Oracle-specific replication configuration.
  def edit()
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Auto-enable replicator at start-up", PV_BOOLEAN, REPL_AUTOENABLE, "true")
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Replicator role (master/slave)", PV_DBMSROLE, REPL_ROLE, "slave")
    
    if @configurator.config.props[REPL_ROLE] == "master" then
      @configurator.config.props[REPL_MASTERHOST] = "localhost"
    else
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Master host name", PV_HOSTNAME, REPL_MASTERHOST, nil)
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Do you have a license to run an Oracle slave that can be promoted to master", PV_HOSTNAME, 
        REPL_ORACLE_LICENSED_SLAVE, "no")
    end
    
    # Masters and slaves that can become masters are treated the same in many ways.
    if (@configurator.config.props[REPL_ROLE] == "master" ||
        @configurator.config.props[REPL_ORACLE_LICENSED_SLAVE] != "no") then
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Oracle Replicator License", PV_ANY, REPL_ORACLE_LICENSE, ENV["REPLICATOR_LICENSE"])
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Source schema name (can be changed in map file)", PV_ANY, REPL_ORACLE_SCHEMA, "SCOTT") 
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "dslisten port", PV_INTEGER, REPL_ORACLE_DSPORT, "51060")         
    end
        
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Oracle Home ($ORACLE_HOME)", PV_ANY, REPL_ORACLE_HOME, ENV["ORACLE_HOME"])
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Oracle Database Service ($ORACLE_SID)", PV_ANY, REPL_ORACLE_SERVICE, ENV["ORACLE_SID"])
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database server port", PV_INTEGER, REPL_DBPORT, "1521")
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database login for Tungsten", PV_IDENTIFIER, REPL_DBLOGIN, "tungsten")
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database password", PV_ANY, REPL_DBPASSWORD, "secret")
  end

  # Perform configuration of Oracle-related files.
  def configure()
    # Configure replicator.properties.
    replicator_properties = "tungsten-replicator/conf/static-" + 
        @configurator.config.props[GLOBAL_DSNAME] + ".properties"
    transformer = Transformer.new(
        "tungsten-replicator/samples/conf/sample.static.properties.oracle",
        replicator_properties, "# ")

    transformer.transform { |line|
      if line =~ /replicator.role=master/ then
          "replicator.role=" + @configurator.config.props[REPL_ROLE]
      elsif line =~ /replicator.auto_enable/ then
          "replicator.auto_enable=" + @configurator.config.props[REPL_AUTOENABLE]
      elsif line =~ /replicator.source_id/ then
          "replicator.source_id=" + @configurator.config.props[GLOBAL_HOST]
      elsif line =~ /cluster.name=/ then
          "cluster.name=" + @configurator.config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /service.name=/ then
          "service.name=" + @configurator.config.props[GLOBAL_DSNAME]
      elsif line =~ /replicator.store.thl.url/ then
          "replicator.store.thl.url=" + "jdbc:oracle:thin:\@localhost:" + @configurator.config.props[REPL_DBPORT] + ":" +
			@configurator.config.props[REPL_ORACLE_SERVICE]
      elsif line =~ /replicator.master.connect.uri/ then
        if @configurator.config.props[REPL_ROLE] == "master" then
          line
        else
          "replicator.master.connect.uri=thl://" + @configurator.config.props[REPL_MASTERHOST] + "/"
        end
      elsif line =~ /replicator.master.listen.uri/ then
          "replicator.master.listen.uri=thl://" + @configurator.config.props[GLOBAL_HOST] + "/"
      elsif line =~ /replicator.extractor.oracle.host/ then
        "replicator.extractor.oracle.host=" + "localhost"
      elsif line =~ /replicator.extractor.oracle.instance/ then
        "replicator.extractor.oracle.instance=" + @configurator.config.props[REPL_ORACLE_SERVICE]
      elsif line =~ /replicator.extractor.oracle.dsport/ then
        "replicator.extractor.oracle.dsport=" + @configurator.config.props[REPL_ORACLE_DSPORT]
      elsif line =~ /replicator.extractor.oracle.user/ then
        "replicator.extractor.oracle.user=" + @configurator.config.props[REPL_DBLOGIN]
      elsif line =~ /replicator.extractor.oracle.password/ then
        "replicator.extractor.oracle.password=" + @configurator.config.props[REPL_DBPASSWORD]

      elsif line =~ /replicator.applier.oracle.host/ then
        "replicator.applier.oracle.host=" + "localhost"
      elsif line =~ /replicator.applier.oracle.port/ then
        "replicator.applier.oracle.port=" + @configurator.config.props[REPL_DBPORT]
      elsif line =~ /replicator.applier.oracle.service/ then
        "replicator.applier.oracle.service=" + @configurator.config.props[REPL_ORACLE_SERVICE]
      elsif line =~ /replicator.applier.oracle.user/ then
        "replicator.applier.oracle.user=" + @configurator.config.props[REPL_DBLOGIN]
      elsif line =~ /replicator.applier.oracle.password/ then
        "replicator.applier.oracle.password=" + @configurator.config.props[REPL_DBPASSWORD]
      else
        line
      end
    }

    # Masters and slaves that can become masters are treated the same in many ways.
    if (@configurator.config.props[REPL_ROLE] == "master" ||
        @configurator.config.props[REPL_ORACLE_LICENSED_SLAVE] != "no") then

      #
      # create tdslisten from tdslisten.sample
      #
      transformer = Transformer.new(
          "tungsten-replicator-oracle/bin/tdslisten.sample",
          "tungsten-replicator-oracle/bin/tdslisten", "# ")

      transformer.transform { |line|
        if line =~ /export ORACLE_HOME/ then
          "export ORACLE_HOME=" + @configurator.config.props[REPL_ORACLE_HOME]
        elsif line =~ /export ORACLE_SID/ then
          "export ORACLE_SID=" + @configurator.config.props[REPL_ORACLE_SERVICE]
        elsif line =~ /export LD_LIBRARY_PATH/ then
          "export LD_LIBRARY_PATH=\"$LD_LIBARY_PATH\":" + @configurator.config.props[REPL_ORACLE_HOME] + "/lib"
        elsif line =~ /export REPLICATOR_HOME/ then
          "export REPLICATOR_HOME=" + Dir.getwd + "/tungsten-replicator-oracle"
        elsif line =~ /export REPLICATOR_LICENSE/ then
          "export REPLICATOR_LICENSE=\"" + @configurator.config.props[REPL_ORACLE_LICENSE] + "\""
        else
          line
        end
      }
      File.chmod(0775, "tungsten-replicator-oracle/bin/tdslisten")
    
      #
      # create mapfile from initSAMPLE.map
      #
      transformer = Transformer.new(
        "tungsten-replicator-oracle/conf/dbs/tableSAMPLE.map",
        "tungsten-replicator-oracle/conf/dbs/table" + @configurator.config.props[REPL_ORACLE_SERVICE] + ".map",
        "# ")

      transformer.transform { |line|
        if line =~ /SAMPLE_SCHEMA/ then
          @configurator.config.props[REPL_ORACLE_SCHEMA] + ".*->" + @configurator.config.props[REPL_ORACLE_SCHEMA] + ".*"
        else
          line
        end
      }
    end    
  end

  # Perform installation of required replication support files.
  def post_configure()
    puts <<POST_CONFIGURE_NOTES
Oracle installation successful.
POST_CONFIGURE_NOTES

    # Masters and slaves that can become masters are treated the same in many ways.
    if (@configurator.config.props[REPL_ROLE] == "master" ||
        @configurator.config.props[REPL_ORACLE_LICENSED_SLAVE] != "no") then

      # augment deployall
      svc = "tungsten-replicator-oracle/bin/tdslisten"
      script = "cluster-home/bin/deployall"
      out = File.open(script, "a")
      out.puts "# Augment deployall to support Oracle extractor on #{DateTime.now}"
      svcname = File.basename svc
      out.puts "ln -fs $PWD/" + svc + " /etc/init.d/" + svcname
      out.puts "/sbin/chkconfig --add " + svcname
      out.chmod(0755)
      out.close
      puts ">> AUGMENTED FILE: " + script

      # augment undeployall
      svc = "tungsten-replicator-oracle/bin/tdslisten"
      script = "cluster-home/bin/undeployall"
      out = File.open(script, "a")
      out.puts "# Augment undeployall to support Oracle extractor on #{DateTime.now}"
      svcname = File.basename svc
      out.puts "/sbin/chkconfig --del " + svcname
      out.puts "\\rm -f /etc/init.d/" + svcname
      out.chmod(0755)
      out.close
      puts ">> AUGMENTED FILE: " + script
    end
    
    if (@configurator.config.props[REPL_ROLE] == "slave" &&
        @configurator.config.props[REPL_ORACLE_LICENSED_SLAVE] == "no") then
        puts <<UNLICENSED_SLAVE_WARNING
---------------------------------------------------------------------------------------------------------
NOTE:  You have installed a slave that is not licensed to become a master.  Should this slave be asked to
become master, it will exit in error thereby disabling its ability to serve as a slave until restarted.
---------------------------------------------------------------------------------------------------------
UNLICENSED_SLAVE_WARNING
        
    end
  end

  # Offer post-configuration advice.
  def print_usage()
    puts <<ORACLE_ADVICE
Oracle configuration instructions are located in Chapter 2 of the
Replicator Guide.  Note that you may have to configure Oracle as
well as the replicator.  The configuration script does not automatically
create user logins or the Tungsten Replicator 'tungsten' database.
ORACLE_ADVICE

  end
end
