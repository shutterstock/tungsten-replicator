# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

require 'tungsten/system_require'

system_require 'open3'
require 'tungsten/subconfigurator'

# Base class for handling subconfiguration of PostgreSQL replication.
class SubConfiguratorPostgreSQL  < SubConfigurator
  include ParameterNames

  # PostgreSQL-specific property validators.
  PV_PG_REPLICATOR = PropertyValidator.new("wal|londiste",
  "Value must be wal (warm standby replication) or londiste (Londiste replication)")
  PV_PG_WAL_DBMS_ROLE = PropertyValidator.new("master|slave",
  "Value must be master or slave")
  PV_PG_BACKUP_METHOD = PropertyValidator.new("none|pg_dump|lvm|script",
  "Value must be none, pg_dump, lvm, or script")
  def initialize(configurator)
    @configurator = configurator
  end

  # Early system prerequisite checks.
  def pre_checks()
    warn = false
    # Ensure we are using a good login for PostgreSQL.
    login = (ENV['USERNAME'] or ENV['USER'])
    if @configurator.config.props[GLOBAL_DBMS_TYPE] == "postgresql" then
      if login != "postgres" and login != "enterprisedb" then
        puts "WARNING: you must extract and configure Tungsten for PostgreSQL with"
        puts "         user 'postgres' and EnterpriseDB with user 'enterprisedb'."
        puts "Your current user is: " + login
        puts "We recommend you do not use this user."
        warn = true
      end
      if @configurator.config.props[GLOBAL_USERID] != "postgres" and @configurator.config.props[GLOBAL_USERID] != "enterprisedb" then
        puts "WARNING: you must run Tungsten software for PostgreSQL with"
        puts "         user 'postgres' and EnterpriseDB with user 'enterprisedb'."
        puts "You have specified: " + @configurator.config.props[GLOBAL_USERID]
        puts "We recommend you do not use this user."
        warn = true
      end
    end
    # Halt for user confirmation.
    if warn
      @configurator.halt_confirm
    end
  end

  # Find out the full executable path or return nil
  # if this is not executable.
  def which(cmd)
    if ! cmd
      nil
    else
      path = `which #{cmd} 2> /dev/null`
      path.chomp!
      if File.executable?(path)
        path
      else
        nil
      end
    end
  end

  # Checks whether the given command exists. Linux only. Silent.
  def command_exists? (cmd)
    stdin, stdout, stderr = Open3.popen3("whereis " + cmd)
    out = ""
    stdout.each do |line|
      out = line
    end
    first = out.index(cmd)
    second = out.index(cmd, first + 2)
    if first and second
      return true
    else
      return false
    end
  end

  def wal_pre_checks()
    # TODO: add any additional prechecks for WAL shipping.
  end

  # Perform pre-configuration checks.
  def pre_configure()
    if @configurator.options.interactive == true
      if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
        # WAL installation has to reboot server.
        if @configurator.config.props[GLOBAL_RESTART_DBMS] == "true"
          puts ""
          puts "WARNING:  PostgreSQL replicator configuration requires a reboot of the PostgreSQL server"
          puts "If you defer the reboot of the PostgreSQL server, a manual reboot prior to starting"
          puts "The Tungsten services"
          if (!@configurator.confirmed("Answer 'y' to reboot during this configuration, 'n' to defer", "y"))
            @configurator.config.props[GLOBAL_RESTART_DBMS] = "defer"
          end
        end
      end
    end
  end

  # Query PG for one field and one row result.
  def psql_atom_query_silent(sql)
    `echo "#{sql}" 2>/dev/null | psql -q -A -t 2>/dev/null`
  end

  # Edit configuration items used by PG-specific replication configuration.
  def edit()
    #@configurator.edit_cfg_value PropertyDescriptor.new(
    #  "PostgreSQL replication method (wal|londiste)", PV_PG_REPLICATOR,
    #  REPL_PG_REPLICATOR, "wal")
    @configurator.config.props[REPL_PG_REPLICATOR] = "wal"

    # Properties differ according to whether you are londiste or wal method.
    if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Use streaming replication (available from PostgreSQL 9)",
      PV_BOOLEAN, REPL_PG_STREAMING, "false")
      wal_pre_checks
      # WAL properties.
      @configurator.edit_cfg_value(PropertyDescriptor.new(
      "Replicator role (master|slave) where slave=standby",
      PV_PG_WAL_DBMS_ROLE, REPL_ROLE, "master"))
      @configurator.config.props[GLOBAL_RESTART_DBMS] = "true"
    else
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Replicator role (master|slave)", PV_DBMSROLE, REPL_ROLE, "master")
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database to be replicated using Londiste", PV_IDENTIFIER,
      REPL_PG_LOND_DATABASE, "")
    end

    # Generic properties from here on out...
    if @configurator.config.props[REPL_ROLE] != "master" then
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Master host name", PV_HOSTNAME, REPL_MASTERHOST, nil)
    else
      @configurator.config.props[REPL_MASTERHOST] = ""
    end
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Auto-enable replicator at start-up", PV_BOOLEAN, REPL_AUTOENABLE, "true")
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Database server port", PV_INTEGER, REPL_DBPORT, "5432")
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Database login for Tungsten services/replication", PV_IDENTIFIER, REPL_DBLOGIN, "postgres")
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Database password", PV_ANY, REPL_DBPASSWORD, "secret")

    def_root_dir = "/var/lib/pgsql"
    def_data_dir = psql_atom_query_silent("SHOW data_directory;").chomp
    if def_data_dir == nil || !def_data_dir.include?("/") || def_data_dir.include?("psql")
      def_data_dir = nil
    else
      def_root_dir = def_data_dir[0, def_data_dir.rindex('/')]
    end
    def_config_path = psql_atom_query_silent("SHOW config_file;").chomp
    if def_config_path == nil || !def_config_path.include?(".conf") || def_data_dir.include?("psql")
      def_config_path = nil
    end
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Root directory for postgresql installation", PV_WRITABLE_DIR, REPL_PG_ROOT, def_root_dir)

    config_root=@configurator.config.props[REPL_PG_ROOT]

    @configurator.edit_cfg_value PropertyDescriptor.new(
    "PostgreSQL data directory", PV_WRITABLE_DIR, REPL_PG_HOME,  def_data_dir)
    if def_config_path == nil
      def_config_path = config_root + "/data/postgresql.conf"
    end
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Location of postgresql.conf", PV_WRITABLE_FILE, REPL_PG_POSTGRESQL_CONF, def_config_path)

    @configurator.edit_cfg_value PropertyDescriptor.new(
    "PostgreSQL archive location", PV_WRITABLE_DIR, REPL_PG_ARCHIVE,
    config_root + "/archive")

    @configurator.edit_cfg_value PropertyDescriptor.new(
    "PostgreSQL start script", PV_EXECUTABLE_FILE, REPL_BOOT_SCRIPT,
    "/etc/init.d/postgresql")

    # Wal buffer timeout is for WAL archiving only.
    if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Timeout for sending unfilled WAL buffers (data loss window)",
      PV_INTEGER, REPL_PG_ARCHIVE_TIMEOUT, "60")
    end

    # Configure backups.
    @configurator.edit_cfg_value PropertyDescriptor.new(
    "Database backup method (none|pg_dump|lvm|script)",
    PV_PG_BACKUP_METHOD, REPL_BACKUP_METHOD, "none")
    if @configurator.config.props[REPL_BACKUP_METHOD] != "none"
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Backup permanent shared storage",
      PV_WRITABLE_DIR, REPL_BACKUP_STORAGE_DIR, "/mnt/backup")
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Backup temporary dump directory",
      PV_WRITABLE_DIR, REPL_BACKUP_DUMP_DIR, "/tmp")
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Number of backups to retain", PV_INTEGER, REPL_BACKUP_RETENTION, "3")
    else
      # Set defaults.
      @configurator.set_cfg_default(REPL_BACKUP_STORAGE_DIR, "/mnt/backup")
      @configurator.set_cfg_default(REPL_BACKUP_DUMP_DIR, "/tmp/backup")
      @configurator.set_cfg_default(REPL_BACKUP_RETENTION, "3")
    end

    if @configurator.config.props[REPL_BACKUP_METHOD] == "script"
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "What is the path to the backup script",
      PV_EXECUTABLE_FILE, REPL_BACKUP_SCRIPT, "")
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "What is the command prefix required for this script (typically sudo)",
      PV_ANY, REPL_BACKUP_COMMAND_PREFIX, "")
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Does this script support backing up a datasource while it is ONLINE?",
      PV_BOOLEAN, REPL_BACKUP_ONLINE, "false")
    else
      @configurator.set_cfg_default(REPL_BACKUP_SCRIPT, "")
      @configurator.set_cfg_default(REPL_BACKUP_COMMAND_PREFIX, "")
      @configurator.set_cfg_default(REPL_BACKUP_ONLINE, "")
    end
  end

  # Perform configuration of PostgreSQL-related files.
  def configure()
    # Configure replicator.properties - this sets up the plugin.
    replicator_properties = "tungsten-replicator/conf/static-" + 
        @configurator.config.props[GLOBAL_DSNAME] + ".properties"
    transformer = Transformer.new(
        "tungsten-replicator/conf/sample.static.properties.postgresql",
        replicator_properties, "# ")

    transformer.transform { |line|
      if line =~ /replicator.role=master/ then
        "replicator.role=" + @configurator.config.props[REPL_ROLE]
      elsif line =~ /replicator.master.uri/ then
        if @configurator.config.props[REPL_MASTERHOST]
          "replicator.master.uri=wal://" + @configurator.config.props[REPL_MASTERHOST] + "/"
        else
          "replicator.master.uri=wal://localhost/"
        end
      elsif line =~ /replicator.master.connect.uri/ then
        if @configurator.config.props[REPL_ROLE] == "master" then
          line
        else
          "replicator.master.connect.uri=thl://" + @configurator.config.props[REPL_MASTERHOST] + "/"
        end
      elsif line =~ /replicator.master.listen.uri/ then
        "replicator.master.listen.uri=thl://" + @configurator.config.props[GLOBAL_HOST] + "/"
      elsif line =~ /replicator.auto_enable/ then
        "replicator.auto_enable=" + @configurator.config.props[REPL_AUTOENABLE]
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @configurator.config.props[GLOBAL_HOST]
      elsif line =~ /cluster.name=/ then
          "cluster.name=" + @configurator.config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /service.name=/ then
          "service.name=" + @configurator.config.props[GLOBAL_DSNAME]
      elsif line =~ /replicator.resourceJdbcUrl/
        line.sub("@HOSTNAME@", @configurator.config.props[GLOBAL_HOST])
      elsif line =~ /replicator.backup.agents/
        if @configurator.config.props[REPL_BACKUP_METHOD] == "none"
          "replicator.backup.agents="
        else
          "replicator.backup.agents=" + @configurator.config.props[REPL_BACKUP_METHOD]
        end
      elsif line =~ /replicator.backup.default/
        if @configurator.config.props[REPL_BACKUP_METHOD] == "none"
          "replicator.backup.default="
        else
          "replicator.backup.default=" + @configurator.config.props[REPL_BACKUP_METHOD]
        end
      elsif line =~ /replicator.backup.agent.pg_dump.port/
        "replicator.backup.agent.pg_dump.port=" + @configurator.config.props[REPL_DBPORT]
      elsif line =~ /replicator.backup.agent.pg_dump.user/
        "replicator.backup.agent.pg_dump.user=" + @configurator.config.props[REPL_DBLOGIN]
      elsif line =~ /replicator.backup.agent.pg_dump.password/
        "replicator.backup.agent.pg_dump.password=" + @configurator.config.props[REPL_DBPASSWORD]
      elsif line =~ /replicator.backup.agent.pg_dump.dumpDir/
        "replicator.backup.agent.pg_dump.dumpDir=" + @configurator.config.props[REPL_BACKUP_DUMP_DIR]
      elsif line =~ /replicator.storage.agents/
        if @configurator.config.props[REPL_BACKUP_METHOD] == "none"
          "replicator.storage.agents="
        else
          "replicator.storage.agents=fs"
        end
      elsif line =~ /replicator.storage.agent.fs.directory/
        "replicator.storage.agent.fs.directory=" + @configurator.config.props[REPL_BACKUP_STORAGE_DIR]
      elsif line =~ /replicator.storage.agent.fs.retention/
        "replicator.storage.agent.fs.retention=" + @configurator.config.props[REPL_BACKUP_RETENTION]
      elsif line =~ /replicator.script.root/
        "replicator.script.root_dir=" + File.expand_path("tungsten-replicator")
      elsif line =~ /replicator.script.conf_file/
        if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
          "replicator.script.conf_file=conf/postgresql-wal.properties"
        else
          "replicator.script.conf_file=conf/postgresql-londiste.ini"
        end
      elsif line =~ /replicator.script.processor/
        if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
          "replicator.script.processor=bin/pg-wal-plugin"
        else
          "replicator.script.processor=bin/pg-londiste-plugin"
        end
      else
        line
      end
    }

    # Configure properties for replication control script.
    if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
      transformer = Transformer.new(
      "tungsten-replicator/conf/sample.postgresql-wal.properties",
      "tungsten-replicator/conf/postgresql-wal.properties", "# ")

      transformer.transform { |line|
        if line =~ /postgresql.streaming_replication/ then
          "postgresql.streaming_replication=" + @configurator.config.props[REPL_PG_STREAMING]
        elsif line =~ /postgresql.data/ then
          "postgresql.data=" + @configurator.config.props[REPL_PG_HOME]
        elsif line =~ /postgresql.conf/ then
          "postgresql.conf=" + @configurator.config.props[REPL_PG_POSTGRESQL_CONF]
        elsif line =~ /^\s*postgresql.pg_standby=\s*(\S*)\s*$/ then
          # For pg_standby to work we need the full path of whatever is in
          # the sample file.
          pg_standby = which $1
          # If that does not work, try looking for the binaries under roo.
          if ! pg_standby
            pg_standby = which(@configurator.config.props[REPL_PG_ROOT] + "/bin/pg_standby")
          end
          if pg_standby
            "postgresql.pg_standby=" + pg_standby
          else
            raise SystemError, "Unable to locate pg_standby; please ensure it is defined correctly in tungsten-replicator/conf/sample.postgresql-wal.properties"
          end
        elsif line =~ /^\s*postgresql.pg_archivecleanup=\s*(\S*)\s*$/ then
          pg_archivecleanup = which $1
          if ! pg_archivecleanup
            pg_archivecleanup = which(@configurator.config.props[REPL_PG_ROOT] + "/bin/pg_archivecleanup")
          end
          if pg_archivecleanup
            "postgresql.pg_archivecleanup=" + pg_archivecleanup
          elsif @configurator.config.props[REPL_PG_STREAMING] == "true"
            raise SystemError, "Unable to locate pg_archivecleanup; please ensure it is defined correctly in tungsten-replicator/conf/sample.postgresql-wal.properties"
          end
        elsif line =~ /postgresql.archive_timeout/ then
          "postgresql.archive_timeout=" + @configurator.config.props[REPL_PG_ARCHIVE_TIMEOUT]
        elsif line =~ /postgresql.archive/ then
          "postgresql.archive=" + @configurator.config.props[REPL_PG_ARCHIVE]
        elsif line =~ /postgresql.role/ then
          "postgresql.role=" + @configurator.config.props[REPL_ROLE]
        elsif line =~ /postgresql.master.host/ then
          if @configurator.config.props[REPL_MASTERHOST]
            "postgresql.master.host=" + @configurator.config.props[REPL_MASTERHOST]
          else
            "postgresql.master.host="
          end
        elsif line =~ /postgresql.boot.script/ then
          "postgresql.boot.script=" + @configurator.config.props[REPL_BOOT_SCRIPT]
        elsif line =~ /postgresql.root.prefix/ then
          "postgresql.root.prefix=" + @configurator.get_root_prefix
        else
          line
        end
      }
    elsif @configurator.config.props[REPL_PG_REPLICATOR] == "londiste"
      # Define Londiste ini file.
      transformer = Transformer.new(
      "tungsten-replicator/conf/sample.postgresql-londiste.ini",
      "tungsten-replicator/conf/postgresql-londiste.ini", "# ")

      transformer.transform { |line|
        if line =~ /base/ then
          "base_directory = conf/pg-londiste/nodes/" + @configurator.config.props[GLOBAL_HOST]
        else
          line
        end
      }
    end

    # Configure wrapper.conf to use the Open Replicator main class.
    transformer = Transformer.new(
    "tungsten-replicator/conf/wrapper.conf",
    "tungsten-replicator/conf/wrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.app.parameter.1/ then
        "wrapper.app.parameter.1=com.continuent.tungsten.replicator.management.OpenReplicatorManager"
      else
        line
      end
    }

    # Configure monitoring for PostgreSQL.
    transformer = Transformer.new(
    "tungsten-monitor/conf/sample.checker.postgresqlserver.properties",
    "tungsten-monitor/conf/checker.postgresqlserver.properties", "# ")

    user = @configurator.config.props[REPL_DBLOGIN]
    password = @configurator.config.props[REPL_DBPASSWORD]

    transformer.transform { |line|
      if line =~ /serverName=/
        "serverName=" + @configurator.config.props[GLOBAL_HOST]
      elsif line =~ /url=/
        "url=jdbc:postgresql://" + @configurator.config.props[GLOBAL_HOST] + ':' + @configurator.config.props[REPL_DBPORT] + "/" + user
      elsif line =~ /frequency=/
        "frequency=" + @configurator.config.props[REPL_MONITOR_INTERVAL]
      elsif line =~ /host=/
        "host=" + @configurator.config.props[GLOBAL_HOST]
      elsif line =~ /username=/
        "username=" + user
      elsif line =~ /password=/
        if password == "" || password == nil then
          "password="
        else
          "password=" + password
        end
      else
        line
      end
    }

    # Create service script for PostgreSQL.
    script = @configurator.config.props[REPL_BOOT_SCRIPT]
    @configurator.write_svc_properties("postgresql", script)
    @configurator.write_mon_extension_properties()
  end

  # Perform installation of required replication support files.
  def post_configure()
    # Select installation command.
    if @configurator.config.props[REPL_PG_REPLICATOR] == "wal"
      puts "Running procedure to configure warm standby..."
      cmd1= "tungsten-replicator/bin/pg-wal-plugin -o uninstall -c tungsten-replicator/conf/postgresql-wal.properties"
      cmd2 = "tungsten-replicator/bin/pg-wal-plugin -o install -c tungsten-replicator/conf/postgresql-wal.properties"
      puts "############ RESTART=#{@configurator.config.props[GLOBAL_RESTART_DBMS]} ##################"
      if (@configurator.config.props[GLOBAL_RESTART_DBMS] == "defer" ||
      @configurator.config.props[GLOBAL_RESTART_DBMS] == "false")
        cmd2 = cmd2 + " -s"
      end

      puts "----------------------------------------------------------------"
      # Do uninstall first.
      exec_cmd2(cmd1, true)
      # Then the install, which has to success.
      exec_cmd2(cmd2, false)
      puts "----------------------------------------------------------------"
    elsif @configurator.config.props[REPL_PG_REPLICATOR] == "londiste"
      puts "Running procedure to configure Londiste replication"
      host = @configurator.config.props[GLOBAL_HOST]
      dbname = @configurator.config.props[REPL_PG_LOND_DATABASE]
      #node_dir = "tungsten-replicator/conf/pg-londiste/nodes/#{host}"
      Dir.chdir("tungsten-replicator")
      node_dir = "conf/pg-londiste/nodes/#{host}"
      #pg_londiste_plugin = "tungsten-replicator/bin/pg-londiste-plugin"
      pg_londiste_plugin = "./bin/pg-londiste-plugin"

      # Remove and recreate node directory.
      exec_cmd2("rm -r #{node_dir}", true)
      exec_cmd2("mkdir -p #{node_dir}", false)

      # Create ini files.
      exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o prepare -I '#{node_dir}:#{host}'", false)
      exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o add_node -I '#{host}:dbname=#{dbname}:#{node_dir}'", false)

      # Configure according to whether we are master or slave.
      if @configurator.config.props[REPL_ROLE] == "master"
        exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o set_master_node -I #{host}", false)
        exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o setrole -I 'role=master'", false)
      else
        master_host = @configurator.config.props[REPL_MASTERHOST]
        exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o add_node -I '#{master_host}:host=#{master_host};dbname=#{dbname}:#{node_dir}'", false)
        exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o set_master_node -I #{master_host}", false)
        exec_cmd2("#{pg_londiste_plugin} -b #{node_dir} -o setrole -I 'role=slave'", false)
      end
      Dir.chdir("..")
    else
      raise SystemError, "Unexpected PostgreSQL replication type: #{@configurator.config.props[REPL_PG_REPLICATOR]}"
    end

  end

  # Offer post-configuration advice.
  def print_usage()
    if @configurator.config.props[GLOBAL_RESTART_DBMS] == "true"
      puts <<PGSQL_ADVICE
PostgreSQL configuration instructions are located in Chapter 4 of the
Tungsten Replicator Guide.  There is no need to configure PostgreSQL as
all configuration should now be complete.  

PGSQL_ADVICE
    else
      if @configurator.config.props[REPL_ROLE] == "master"
        puts <<PGSQL_ADVICE_MASTER
You have chosen to install PostgreSQL warm standby clustering in 
manual mode.  You must reboot your PostgreSQL server and start 
Tungsten services to begin operation.  

As part of configuration we have placed an 'include' command at the 
end of the postgresql.conf located at the following path: 

  #{@configurator.config.props[REPL_PG_POSTGRESQL_CONF]}

We recommend you check the settings in the included file and then start
using the following commands: 

  #{@configurator.get_svc_command(@configurator.config.props[REPL_BOOT_SCRIPT])} stop
  #{@configurator.get_svc_command(@configurator.config.props[REPL_BOOT_SCRIPT])} start
  ./cluster-home/bin/startall
  
PGSQL_ADVICE_MASTER
      else
        puts <<PGSQL_ADVICE_SLAVE
You have chosen to install PostgreSQL warm standby clustering in 
manual mode.  As part of configuration we have placed an 'include'
command at the end of the postgresql.conf located at the following
path:

  #{@configurator.config.props[REPL_PG_POSTGRESQL_CONF]}

We recommend you check the settings in the included file and then start
using the following commands: 

  ./cluster-home/bin/startall

Your PostgreSQL server will provision itself and reboot automatically
when the Tungsten Replicator comes online.

PGSQL_ADVICE_SLAVE
      end
    end

    if @configurator.config.props[REPL_BACKUP_METHOD] == "lvm" || @configurator.config.props[REPL_BACKUP_METHOD] == "script"
      puts <<BACKUP_CONFIGURATION_ADVICE
Backup Setup
============
You have selected a backup method that cannot be set up through
the configuration script.  You will need to edit replicator.properties
directly to complete backup configuration.  Please refer to the
Replicator Guide for additional information on backups.

BACKUP_CONFIGURATION_ADVICE
    end
  end
end
