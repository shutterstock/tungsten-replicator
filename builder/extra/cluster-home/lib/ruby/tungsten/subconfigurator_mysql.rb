# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009-2011 Continuent, Inc.
# All rights reserved
#

require 'tungsten/subconfigurator'

# Base class for handling subconfiguration of MySQL replication.
class SubConfiguratorMySQL < SubConfigurator
  include ParameterNames
  DYNAMIC_PROPERTIES_FILE = "tungsten-replicator/conf/dynamic.properties"

  # MySQL-specific property validators.
  PV_MYSQL_BACKUP_METHOD = PropertyValidator.new(
    "none|mysqldump|lvm|xtrabackup|script",
    "Value must be none, mysqldump, lvm, xtrabackup, or script")

  def initialize(configurator)
    @configurator = configurator
  end

  # Early system prerequisite checks.
  def pre_checks()
    # TODO: create checks for MySQL.
  end

  # Perform pre-configuration checks.
  def pre_configure()
    # Ensure we have default value(s) for recently added configuration
    # parameters.
    if @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] == ""
      if @configurator.config.props[REPL_EXTRACTOR_USE_RELAY_LOGS] == "true"
        @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] = "relay"
      else
        @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] = "direct"
      end
    end
    if @configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] == ""
      @configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] = "false"
    end
    if @configurator.config.props[REPL_MYSQL_BINLOGPATTERN] == ""
      @configurator.config.props[REPL_MYSQL_BINLOGPATTERN] = "mysql-bin"
    end
    if @configurator.config.props[REPL_JAVA_MEM_SIZE] == ""
      @configurator.config.props[REPL_JAVA_MEM_SIZE] = "512"
    end
    if @configurator.config.props[REPL_BUFFER_SIZE] == ""
      @configurator.config.props[REPL_BUFFER_SIZE] = "10"
    end
    if @configurator.config.props[REPL_MYSQL_RO_SLAVE] == ""
      @configurator.config.props[REPL_MYSQL_RO_SLAVE] = "false"
    end
    if @configurator.config.props[REPL_THL_DO_CHECKSUM] == ""
      @configurator.config.props[REPL_THL_DO_CHECKSUM] = "false"
    end
    if @configurator.config.props[REPL_THL_LOG_CONNECTION_TIMEOUT] == ""
      @configurator.config.props[REPL_THL_LOG_CONNECTION_TIMEOUT] = "600"
    end
  end

  # Return the value of the MySQL configuration parameter named
  # mycnfVariable from the my.cnf file identified by mycnfFileName.
  # If mycnfVariable is not found mycnfDefaultValue is returned.
  def findMycnfValue(mycnfFileName, mycnfVariable, mycnfDefaultValue)
    retval = mycnfDefaultValue
    File.open(mycnfFileName) do |file|
      while line = file.gets
        next if line =~ /^#/
        assignmentStatement = line.split(/\s*=\s*/)
        if assignmentStatement.length == 2
          if (assignmentStatement[0] == mycnfVariable)
            # puts line
            retval = assignmentStatement[1]
            retval = retval.strip
          end
        end
      end
    end
    return retval
  end

  def getMySQLConsoleExec()
    return "mysql -u" + @configurator.config.props[REPL_DBLOGIN] +
    " -p" + @configurator.config.props[REPL_DBPASSWORD] +
    " -h" + @configurator.config.props[REPL_DATASERVER_HOST] +
    " -P" + @configurator.config.props[REPL_DBPORT]
  end

  def testMySQLLogin()
    mysqlCmd = getMySQLConsoleExec()
    puts mysqlCmd if @configurator.options.verbose
    return system("echo \"exit\" | " + mysqlCmd)
  end

  # Edit configuration items used by MySQL-specific replication configuration.
  def edit()
    # Compute base_dir used to default values for directories.
    base_dir = File.dirname(Dir.pwd)

    puts
    @configurator.write_divider
    puts <<AUTO_ENABLE
Tungsten Replicator can automatically go online on start-up.  This
is called auto-enabling and is normally the desired behavior.
However, if you are migrating from MySQL Replication or want to
start replication manually, you should not enable this feature.

AUTO_ENABLE

    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Auto-enable replicator at start-up", PV_BOOLEAN, REPL_AUTOENABLE, "true")

    puts
    @configurator.write_divider
    puts <<ROLE
Each replicator has a default role of master or slave.  If you
choose the slave role, you must also supply the master host name.

ROLE
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Replicator role (master|slave|direct|dummy)", PV_DBMSROLE, REPL_ROLE, "slave")
    if @configurator.config.props[REPL_ROLE] == "master" then
      @configurator.config.props[REPL_MASTERHOST] = "localhost"
    elsif @configurator.config.props[REPL_ROLE] == "slave" then
      @configurator.edit_cfg_value PropertyDescriptor.new(
      "Master host name", PV_HOSTNAME, REPL_MASTERHOST, nil)
    else
      @configurator.config.props[REPL_MASTERHOST] = "localhost"
    end

    puts
    @configurator.write_divider
    puts <<DBMS_INFO
Tungsten needs the DBMS server host name and port number as well a login and
password to use when connecting to the database server.  This account
must exist and have full administrative privileges including SUPER
to operate replication.

DBMS_INFO
    @configurator.edit_cfg_value PropertyDescriptor.new("Database server host",
      PV_HOSTNAME, REPL_DATASERVER_HOST, 
      @configurator.config.props[GLOBAL_HOST])
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database server port", PV_INTEGER, REPL_DBPORT, "3306")
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database login for Tungsten", PV_IDENTIFIER, REPL_DBLOGIN, "tungsten")
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database password", PV_ANY, REPL_DBPASSWORD, "secret")

    # Check provided MySQL login.
    if !@configurator.options.isExpert && !testMySQLLogin()
      puts "WARNING: failed to connect to MySQL with the credentials given."
      puts "MySQL must be accessible to the given user '" +
      @configurator.config.props[REPL_DBLOGIN] +
      "' for Tungsten to work."
      answer = read_value "Would you like to create the DB user now?", "yes"
      if answer =~ /yes|YES|y|Y/
        puts "Please provide database login with GRANT privileges:"
        grantUser = read_value "Database login (used only now)", "root"
        grantPass = read_value "Database password", ""

        puts "The following statement will be called via 'mysql':"
        grantSQL = "GRANT ALL PRIVILEGES ON *.* TO '" + @configurator.config.props[REPL_DBLOGIN] +
        "'@'%' IDENTIFIED BY '" + @configurator.config.props[REPL_DBPASSWORD] + "' WITH GRANT OPTION;"
        puts grantSQL

        answer = read_value "Do you confirm executing this on " + 
        @configurator.config.props[REPL_DATASERVER_HOST] + ":" + 
        @configurator.config.props[REPL_DBPORT], "no" 
        if answer =~ /yes|YES|y|Y/
          succeeded = system "echo \"" + grantSQL + "\" | mysql -u" + grantUser +
          " -p" + grantPass + 
          " -h" + @configurator.config.props[REPL_DATASERVER_HOST] + 
          " -P" + @configurator.config.props[REPL_DBPORT]
          if !succeeded
            puts "WARNING: failed to create MySQL login, please configure manually."
            puts @@pressEnterMsg
            STDIN.gets
          else
            puts "Database login '" + @configurator.config.props[REPL_DBLOGIN] + "' created. Testing..."
            if !testMySQLLogin()
              puts "WARNING: DB login creation failed, please configure manually."
            else
              puts "OK"
            end
          end
        else
          puts "WARNING: DB login creation canceled, please configure manually."
        end
      else
        puts "WARNING: DB login creation canceled, please configure manually."
      end
    else
      # Check whether MySQL replication (slave) is not turned on.
      cmd = "echo \"SHOW SLAVE STATUS\\G\" | " + getMySQLConsoleExec()
      out = `#{cmd}`
      if out =~ /Slave_IO_Running: Y/
        puts "WARNING: MySQL built-in replication found to be turned on."
        puts "Please ensure that MySQL slave replicator is turned off or"
        puts "it will conflict with the Tungsten Replicator."
        puts @@pressEnterMsg
        STDIN.gets
      end
    end

    # Collect information on the database.
    puts
    @configurator.write_divider
    puts <<DBMS_BINLOG_EXTRACTION
Tungsten can extract MySQL transactions directly from the on-disk binlogs
(direct) or by downloading binlog files to a temporary directory (relay).  
Select relay extraction if the MySQL server is on another host or the 
local binlog files are NFS-mounted.  Otherwise, select direct extraction. 

DBMS_BINLOG_EXTRACTION

    @configurator.edit_cfg_value PropertyDescriptor.new(
      "MySQL binlog extraction method", PV_BINLOG_EXTRACTION, 
       REPL_MYSQL_EXTRACT_METHOD, "direct")

    # Infer binlog location information if we are local.  Otherwise
    # set the relay log location. 
    if @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] == "direct"
      puts
      @configurator.write_divider
      puts <<DBMS_INFO2
You are configuring Tungsten to extract using local binary logs.  You 
must provide the location of these logs, which must also be readable. 
Tungsten also needs the location of the my.cnf file to determine the 
binary log file pattern.  

DBMS_INFO2

      @configurator.edit_cfg_value PropertyDescriptor.new(
        "MySQL binlog directory", PV_READABLE_DIR, REPL_MYSQL_BINLOGDIR,
        "/var/lib/mysql")

      # Try to set a reasonable default for my.cnf file. 
      #
      mysql_config = "/etc/my.cnf"
      if (!File.exist?(mysql_config))
        if (File.exist?("/etc/mysql/my.cnf"))
          mysql_config = "/etc/mysql/my.cnf"
        end
      end
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "MySQL configuration file", PV_READABLE_FILE, REPL_MYSQL_MYCNF,
        mysql_config)
      # Check whether binlogs are enabled.
      defaultPattern = findMycnfValue(@configurator.config.props[REPL_MYSQL_MYCNF], "log_bin", "");
      defaultPatternB = findMycnfValue(@configurator.config.props[REPL_MYSQL_MYCNF], "log-bin", "");
      if defaultPattern == "" and defaultPatternB ==""
        puts "WARNING: binary logs are not enabled in my.cnf"
        puts "Binary logs must be enabled for Replicator's extractor to work."
        puts "Adding the following line to my.cnf will turn logging on:"
        puts "log-bin=mysql-bin"
        puts @@pressEnterMsg
        STDIN.gets
        defaultPattern = "mysql-bin"
      elsif defaultPatternB != ""
        defaultPattern = defaultPatternB
      end
  
      # MySQL allows binlog patterns to be full directory paths.  Use
      # the base name only and remove any extension as MySQL seems to
      # ignore these as well.
      defaultPattern = File.basename(defaultPattern, ".*")

      @configurator.edit_cfg_value PropertyDescriptor.new(
        "MySQL binlog pattern", PV_ANY, REPL_MYSQL_BINLOGPATTERN,
        defaultPattern)
      # Check whether we can read binlogs.
      binLogGlobPath =
        @configurator.config.props[REPL_MYSQL_BINLOGDIR] + "/" +
        @configurator.config.props[REPL_MYSQL_BINLOGPATTERN] + ".*"
      binLogFiles = Dir.glob(binLogGlobPath)
      if binLogFiles.length > 0
        if ! File.readable?(binLogFiles[0])
          puts "WARNING: binary logs cannot be read by current user:"
          puts binLogFiles[0]
          puts "Binary logs must be accessible for Replicator's extractor to work."
          puts @@pressEnterMsg
          STDIN.gets
        end
      else
        puts "WARNING: no binary logs found under name of"
        puts binLogGlobPath
        puts "Please confirm that binary logs are enabled."
        puts @@pressEnterMsg
        STDIN.gets
      end

      # Set default relay log directory.  
      @configurator.set_cfg_default(REPL_RELAY_LOG_DIR, 
        "/opt/continuent/relay-logs")

    else
      # Configure relay logs.
      puts
      @configurator.write_divider
      puts <<RELAY_LOG
You are configuring Tungsten to extract using downloaded binary
logs, which are also known as Tungsten relay logs.  You must provide a 
non-NFS-mounted, local directory for downloaded files and also provide 
the binlog file pattern.

RELAY_LOG
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Enter the local directory for temporary relay log storage",
        PV_WRITABLE_OUTPUT_DIR, REPL_RELAY_LOG_DIR,
        "#{base_dir}/relay-logs")
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "MySQL binlog pattern", PV_ANY, REPL_MYSQL_BINLOGPATTERN,
        defaultPattern)

      # Set default local binlog location. 
      @configurator.set_cfg_default(REPL_MYSQL_BINLOGDIR, 
        "/var/lib/mysql")
    end

    # Configure the replicator log.
    puts
    @configurator.write_divider
    puts <<DISK_LOG
Tungsten stores database transactions for replication in its own
log.  You may store this log either on disk or in tables in your
DBMS server.  The disk log is highly recommended as it is faster
and self-managing.  If you choose the disk log you must provide a
non-NFS, local directory in which to store log files.

DISK_LOG
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, REPL_LOG_TYPE, "disk")
    if @configurator.config.props[REPL_LOG_TYPE] == "disk"
      @configurator.edit_cfg_value PropertyDescriptor.new(
     "Replicator log directory",
      PV_WRITABLE_OUTPUT_DIR, REPL_LOG_DIR, "#{base_dir}/logs")
    end
    
    # Configure backups.
    puts
    @configurator.write_divider
    puts <<BACKUP
Tungsten integrates with a variety of backup mechanisms.  We strongly
recommend you configure one of these to help with provisioning
servers.  Please consult the Tungsten Replicator Guide for more
information on backup configuration.  
    
BACKUP
    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Database backup method (none|mysqldump|lvm|xtrabackup|script)",
      PV_MYSQL_BACKUP_METHOD, REPL_BACKUP_METHOD, "none")
    if @configurator.config.props[REPL_BACKUP_METHOD] != "none"
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Backup permanent shared storage",
        PV_WRITABLE_OUTPUT_DIR, REPL_BACKUP_STORAGE_DIR, "#{base_dir}/backups")
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Backup temporary dump directory",
        PV_WRITABLE_DIR, REPL_BACKUP_DUMP_DIR, "/tmp")
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Number of backups to retain", PV_INTEGER, REPL_BACKUP_RETENTION, "3")
			
      if @configurator.config.props[REPL_ROLE] != "master" then
        @configurator.edit_cfg_value PropertyDescriptor.new(
          "Do you want to provision this machine using the most recent backup?\nIf a backup does not exist, the replicator will not start correctly.", PV_BOOLEAN, REPL_BACKUP_AUTO_PROVISION, "false")
      else
        @configurator.edit_cfg_value PropertyDescriptor.new(
          "Do you want this master to create an initial backup before starting?", PV_BOOLEAN, REPL_BACKUP_AUTO_BACKUP, "false")
      end
    else
      # Set defaults.
      @configurator.set_cfg_default(REPL_BACKUP_STORAGE_DIR, "#{base_dir}/backup")
      @configurator.set_cfg_default(REPL_BACKUP_DUMP_DIR, "#{base_dir}/backup")
      @configurator.set_cfg_default(REPL_BACKUP_RETENTION, "3")
      @configurator.set_cfg_default(REPL_BACKUP_AUTO_PROVISION, "false")
      @configurator.set_cfg_default(REPL_BACKUP_AUTO_BACKUP, "false")
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
    
    # Use bytes for string
    puts
    @configurator.write_divider
    puts <<BINARY_XFER
MySQL allows statements to contain binary data as well as alternate
character sets.  Table columns and tables may contain different
character sets from the system default.  If any of these apply, you
should enable binary transfer.  If you are replicating to another
database type like PostgreSQL you should not enable this feature.

BINARY_XFER

    @configurator.edit_cfg_value PropertyDescriptor.new(
      "Transfer string data using binary format",
      PV_BOOLEAN, REPL_USE_BYTES, "true")

    if @configurator.config.props[REPL_USE_BYTES] == "true" then
      @configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] = "false"
    end

    # Set performance parameters.  We also print a warning if the values
    # seem likely to cause an out-of-memory condition.  At a minimum
    # Java heap memory should be 8 X buffer_size MB.
    puts
    @configurator.write_divider
    puts <<MEMSIZE
Replicators require at least 128mb of heap memory to operate.  You
should allocate as much as you can spare.  Block commit speeds up
replication but requires more heap space to keep transactions in
memory.  If you are using MyISAM table type, set the block commit
size to 1 to avoid data corruption if your slave crashes.

MEMSIZE
    mem_ok = false
    until mem_ok
      # Collect data.
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Replicator Java heap memory size in Mb (min 128)",
        PV_JAVA_MEM_SIZE, REPL_JAVA_MEM_SIZE, "512")
      @configurator.edit_cfg_value PropertyDescriptor.new(
        "Replicator block commit size (min 1, max 100)",
        PV_REPL_BUFFER_SIZE, REPL_BUFFER_SIZE, "10")

      # Check memory to see if it fits in guidelines.
      buffer_size = @configurator.config.props[REPL_BUFFER_SIZE].to_i
      java_heap = @configurator.config.props[REPL_JAVA_MEM_SIZE].to_i
      min_buf_mem = buffer_size * 8
      if min_buf_mem > java_heap
        recommended_buffer_size = java_heap / 8
        puts "WARNING:  Your buffer size may lead to out-of-memory conditions"
        puts "Recommended max buffer size for #{java_heap}MB is #{recommended_buffer_size}"
        mem_ok = @configurator.confirmed("Accept these values anyway?", "n")
      else
        mem_ok = true
      end
    end
  end

  # Perform configuration of MySQL-related files.
  def configure()
    # Generate the correct template based on whether we are using drizzle or
    # J/Connect driver. 
    if (@configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] == "false")
      transformer = Transformer.new(
        "tungsten-replicator/conf/replicator.properties.mysql-with-drizzle-driver",
        "tungsten-replicator/conf/replicator.properties.service.template", "#")
    else
      transformer = Transformer.new(
        "tungsten-replicator/conf/replicator.properties.mysql",
        "tungsten-replicator/conf/replicator.properties.service.template", "#")
    end

    # Configure template file. 
    transformer.transform { |line|
      if line =~ /replicator.role=/ then
        "replicator.role=" + @configurator.config.props[REPL_ROLE]
      elsif line =~ /replicator.global.db.host=/ then
        "replicator.global.db.host=" + @configurator.config.props[REPL_DATASERVER_HOST]
      elsif line =~ /replicator.global.db.port=/ then
        "replicator.global.db.port=" + @configurator.config.props[REPL_DBPORT]
      elsif line =~ /replicator.global.db.user=/ then
        "replicator.global.db.user=" + @configurator.config.props[REPL_DBLOGIN]
      elsif line =~ /replicator.global.db.password=/ then
        "replicator.global.db.password=" + @configurator.config.props[REPL_DBPASSWORD]
      elsif line =~ /replicator.auto_enable/ then
        "replicator.auto_enable=" + @configurator.config.props[REPL_AUTOENABLE]
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @configurator.config.props[GLOBAL_HOST]
      elsif line =~ /cluster.name=/ then
        "cluster.name=" + @configurator.config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /local.service.name=/ then
        "local.service.name=" + @configurator.config.props[GLOBAL_DSNAME]
      elsif line =~ /service.name=/ then
        "service.name=" + @configurator.config.props[GLOBAL_DSNAME]
      elsif line =~ /replicator.service.type=/ then
        "replicator.service.type=local"
      elsif line =~ /replicator.global.buffer.size=/ then
        "replicator.global.buffer.size=" + @configurator.config.props[REPL_BUFFER_SIZE]
      elsif line =~ /replicator.store.thl.url/ then
        if (@configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] == "true")
          "replicator.store.thl.url=jdbc:mysql://" +
          @configurator.config.props[GLOBAL_HOST] + ":" +
          @configurator.config.props[REPL_DBPORT] +
          "/tungsten_${service.name}?createDatabaseIfNotExist=true"
        else
          "replicator.store.thl.url=jdbc:mysql:thin://" +
          @configurator.config.props[GLOBAL_HOST] + ":" +
          @configurator.config.props[REPL_DBPORT] +
          "/tungsten_${service.name}?createDB=true"
        end
      elsif line =~ /replicator.store.thl=/
        if @configurator.config.props[REPL_LOG_TYPE] == "disk" then
          "replicator.store.thl=com.continuent.tungsten.enterprise.replicator.thl.EnterpriseTHL"
        else
          "replicator.store.thl=com.continuent.tungsten.replicator.thl.THL"
        end
      elsif line =~ /replicator.store.thl.storage=/
        if @configurator.config.props[REPL_LOG_TYPE] == "disk" then
          "replicator.store.thl.storage=com.continuent.tungsten.enterprise.replicator.thl.DiskTHLStorage"
        else
          "replicator.store.thl.storage=com.continuent.tungsten.replicator.thl.JdbcTHLStorage"
        end
      elsif line =~ /replicator.store.thl.log_dir=/
        if @configurator.config.props[REPL_LOG_TYPE] == "disk" then
          "replicator.store.thl.log_dir=" + @configurator.config.props[REPL_LOG_DIR]
        else
          "#" + line
        end
      elsif line =~ /replicator.master.connect.uri=/ then
        if @configurator.config.props[REPL_ROLE] == "master" then
          line
        else
          "replicator.master.connect.uri=thl://" + @configurator.config.props[REPL_MASTERHOST] + "/"
        end
      elsif line =~ /replicator.master.listen.uri=/ then
        "replicator.master.listen.uri=thl://" + @configurator.config.props[GLOBAL_HOST] + "/"
      elsif line =~ /replicator.extractor.mysql.binlog_dir/ then
        "replicator.extractor.mysql.binlog_dir=" + @configurator.config.props[REPL_MYSQL_BINLOGDIR]
      elsif line =~ /replicator.resourceLogDir/ then
        "replicator.resourceLogDir=" + @configurator.config.props[REPL_MYSQL_BINLOGDIR]
      elsif line =~ /replicator.resourceLogPattern/ then
        "replicator.resourceLogPattern=" + @configurator.config.props[REPL_MYSQL_BINLOGPATTERN]
      elsif line =~ /replicator.resourceDataServerHost/ then
        "replicator.resourceDataServerHost=" + @configurator.config.props[REPL_DATASERVER_HOST]
      elsif line =~ /replicator.extractor.mysql.binlog_file_pattern/ then
        "replicator.extractor.mysql.binlog_file_pattern=" + @configurator.config.props[REPL_MYSQL_BINLOGPATTERN]
      elsif line =~ /replicator.resourceJdbcUrl/
        line = line.sub("@HOSTNAME@", @configurator.config.props[REPL_DATASERVER_HOST] + ":" +
        @configurator.config.props[REPL_DBPORT])
      elsif line =~ /replicator.resourcePort/ then
        "replicator.resourcePort=" + @configurator.config.props[REPL_DBPORT]
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
      elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/
        "replicator.backup.agent.mysqldump.dumpDir=" + @configurator.config.props[REPL_BACKUP_DUMP_DIR]
      elsif line =~ /replicator.backup.agent.lvm.dumpDir/
        "replicator.backup.agent.lvm.dumpDir=" + @configurator.config.props[REPL_BACKUP_DUMP_DIR]
      elsif line =~ /replicator.backup.agent.lvm.dataDir/
        "replicator.backup.agent.lvm.dataDir=" + @configurator.config.props[REPL_MYSQL_BINLOGDIR]
      elsif line =~ /replicator.backup.agent.script.script/ && @configurator.config.props[REPL_BACKUP_METHOD] == "script"
        "replicator.backup.agent.script.script=" + @configurator.config.props[REPL_BACKUP_SCRIPT]
      elsif line =~ /replicator.backup.agent.script.commandPrefix/ && @configurator.config.props[REPL_BACKUP_METHOD] == "script"
        "replicator.backup.agent.script.commandPrefix=" + @configurator.config.props[REPL_BACKUP_COMMAND_PREFIX]
      elsif line =~ /replicator.backup.agent.script.hotBackupEnabled/ && @configurator.config.props[REPL_BACKUP_METHOD] == "script"
        "replicator.backup.agent.script.hotBackupEnabled=" + @configurator.config.props[REPL_BACKUP_ONLINE]
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
      elsif line =~ /replicator.extractor.mysql.usingBytesForString/
      "replicator.extractor.mysql.usingBytesForString=" + @configurator.config.props[REPL_USE_BYTES]
      elsif line =~ /replicator.vipInterface/
        "replicator.vipInterface=" + @configurator.config.props[REPL_MASTER_VIP_DEVICE]
      elsif line =~ /replicator.vipAddress/
        "replicator.vipAddress=" + @configurator.config.props[REPL_MASTER_VIP]
      elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] == "relay"
        "replicator.extractor.mysql.useRelayLogs=true"
      elsif line =~ /replicator.extractor.mysql.relayLogDir/ && @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] == "relay"
        "replicator.extractor.mysql.relayLogDir=" + @configurator.config.props[REPL_RELAY_LOG_DIR]
      elsif line =~ /replicator.extractor.mysql.relayLogRetention/ && @configurator.config.props[REPL_MYSQL_EXTRACT_METHOD] == "relay"
        "replicator.extractor.mysql.relayLogRetention=3" 
      elsif line =~ /replicator.store.thl.log_file_retention/ && @configurator.config.props[REPL_THL_LOG_RETENTION] != ""
        "replicator.store.thl.log_file_retention=#{@configurator.config.props[REPL_THL_LOG_RETENTION]}"
      elsif line =~ /replicator.applier.consistency_policy/
        "replicator.applier.consistency_policy=#{@configurator.config.props[REPL_CONSISTENCY_POLICY]}"
      elsif line =~ /replicator.store.thl.doChecksum/
        "replicator.store.thl.doChecksum=#{@configurator.config.props[REPL_THL_DO_CHECKSUM]}"
      elsif line =~ /replicator.store.thl.logConnectionTimeout/
        "replicator.store.thl.logConnectionTimeout=#{@configurator.config.props[REPL_THL_LOG_CONNECTION_TIMEOUT]}"
      elsif line =~ /replicator.store.thl.log_file_size/
        "replicator.store.thl.log_file_size=#{@configurator.config.props[REPL_THL_LOG_FILE_SIZE]}"
      else
        line
      end
    }

    # Generate the services.properties file.
    transformer = Transformer.new(
    "tungsten-replicator/conf/sample.services.properties",
    "tungsten-replicator/conf/services.properties", "#")

    transformer.transform { |line|
      if line =~ /replicator.services/
        "replicator.services=" + @configurator.config.props[GLOBAL_DSNAME]
      elsif line =~ /replicator.global.db.user=/ then
        "replicator.global.db.user=" + @configurator.config.props[REPL_DBLOGIN]
      elsif line =~ /replicator.global.db.password=/ then
        "replicator.global.db.password=" + @configurator.config.props[REPL_DBPASSWORD]
      elsif line =~ /replicator.resourceLogDir/ then
        "replicator.resourceLogDir=" + @configurator.config.props[REPL_MYSQL_BINLOGDIR]
      elsif line =~ /replicator.resourceLogPattern/ then
        "replicator.resourceLogPattern=" + @configurator.config.props[REPL_MYSQL_BINLOGPATTERN]
      elsif line =~ /replicator.resourceJdbcUrl/
        line = line.sub("$serviceFacet.name$", @configurator.config.props[REPL_DATASERVER_HOST] + ":" +
        @configurator.config.props[REPL_DBPORT])
      elsif line =~ /replicator.resourceDataServerHost/ then
        "replicator.resourceDataServerHost=" + @configurator.config.props[REPL_DATASERVER_HOST]
      elsif line =~ /replicator.resourceJdbcDriver/ then
        "replicator.resourceJdbcDriver=com.mysql.jdbc.Driver"
      elsif line =~ /replicator.resourcePort/ then
        "replicator.resourcePort=" + @configurator.config.props[REPL_DBPORT]
      elsif line =~ /replicator.resourceDiskLogDir/ then
        "replicator.resourceDiskLogDir=" + @configurator.config.props[REPL_LOG_DIR]
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @configurator.config.props[REPL_DATASERVER_HOST]
      elsif line =~ /replicator.resourceVendor/ then
        "replicator.resourceVendor=" + @configurator.config.props[GLOBAL_DBMS_TYPE]
      elsif line =~ /cluster.name=/ then
        "cluster.name=" + @configurator.config.props[GLOBAL_CLUSTERNAME]
      elsif line =~ /replicator.host=/ then
        "replicator.host=" + @configurator.config.props[GLOBAL_HOST]
      else
        line
      end
    }

    # Configure monitoring for MySQL.
    transformer = Transformer.new(
    "tungsten-monitor/conf/sample.checker.mysqlserver.properties",
    "tungsten-monitor/conf/checker.mysqlserver.properties", "# ")

    user = @configurator.config.props[REPL_DBLOGIN]
    password = @configurator.config.props[REPL_DBPASSWORD]

    if @configurator.config.props[REPL_MONITOR_ACTIVE] =~ /true/
      transformer.transform { |line|
        if line =~ /serverName=/
        "serverName=" + @configurator.config.props[REPL_DATASERVER_HOST]
        elsif line =~ /url=/
        "url=jdbc:mysql://" + @configurator.config.props[REPL_DATASERVER_HOST] + ':' + @configurator.config.props[REPL_DBPORT] + "?jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&allowMultiQueries=true&yearIsDateType=false"
        elsif line =~ /frequency=/
        "frequency=" + @configurator.config.props[REPL_MONITOR_INTERVAL]
        elsif line =~ /host=/
        "host=" + @configurator.config.props[REPL_DATASERVER_HOST]
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
    end
    # Create service properties script for MySQL.
    script = @configurator.config.props[REPL_BOOT_SCRIPT]
    @configurator.write_svc_properties("mysql", script)
    @configurator.write_mon_extension_properties()

    # Create service properties for mysql_readonly script.
    service_mysql_ro = "tungsten-cluster-manager/rules-ext/mysql_readonly.service.properties"
    if (File.exist?(service_mysql_ro))
      FileUtils.cp(service_mysql_ro, "cluster-home/conf/cluster/" + @configurator.config.props[GLOBAL_DSNAME] + "/service/mysql_readonly.properties")
    end
    
    # Generate the MySQL read-only script.  This must have the Tungsten
    # admin user and password.
    user = @configurator.config.props[REPL_DBLOGIN]
    pw   = @configurator.config.props[REPL_DBPASSWORD]
    script = "cluster-home/bin/mysql_readonly"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Make MySQL read-only"
    out.puts "mysql -u#{user} -p#{pw} -e \"SET GLOBAL read_only = $1;\""
    out.puts "mysql -u#{user} -p#{pw} -e \"SHOW VARIABLES LIKE '%read_only%';\""
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    puts ">> GENERATED FILE: " + script
    
    # Deploy user's specified MySQL Connector/J (TENT-222).
    if @configurator.config.props[GLOBAL_DBMS_TYPE] == "mysql" &&
       @configurator.config.props[GLOBAL_USE_MYSQL_CONNECTOR] == "true"

      puts "Deploying user MySQL Connector/J..."
      connector = @configurator.config.props[GLOBAL_MYSQL_CONNECTOR_PATH]
      if connector != nil and connector != ""
        FileUtils.cp(connector, "bristlecone/lib-ext/")
        FileUtils.cp(connector, "tungsten-replicator/lib/")
        else
        puts "FATAL: no MySQL Connector/J JAR specified!"
        puts "You should re-run configure and provide a valid path"
        puts "for the MySQL Connector/J binary.  You will not be able"
        puts "to run Tungsten without this binary."
        puts "Exiting..."
        exit 1
      end
    end
    
    # Configure auto-provisioning
    @dynamic_properties = Properties.new
    if File.exist?(DYNAMIC_PROPERTIES_FILE)
      @dynamic_properties.load(DYNAMIC_PROPERTIES_FILE)
    end
    @dynamic_properties.setProperty("replicator.auto_provision", @configurator.config.props[REPL_BACKUP_AUTO_PROVISION])
    @dynamic_properties.setProperty("replicator.auto_backup", @configurator.config.props[REPL_BACKUP_AUTO_BACKUP])
    @dynamic_properties.store(DYNAMIC_PROPERTIES_FILE)
    
  end

  # Perform post-configuration actions.  This includes starting services,
  # etc.
  def post_configure()
    # Currently a no-op for MySQL.
  end

  # Offer post-configuration advice.
  def print_usage()
    puts <<MYSQL_ADVICE
additional MySQL configuration instructions are located in Chapter 2 of the
Replicator Guide.  

MYSQL_ADVICE

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
