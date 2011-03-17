system_require "configure/deployment_steps/deployment_step_replicator"
module ConfigureDeploymentStepPostgresql
  include ConfigureDeploymentStepReplicator
  
  # The deploy_replicator method is defined in ConfigureDeploymentStepReplicator
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_replicator"),
      ConfigureDeploymentMethod.new("postgresql_configuration", ConfigureDeployment::FINAL_STEP_WEIGHT-1)
    ]
  end
  module_function :get_deployment_methods
  
  def apply_config_replicator
    write_replicator_properties()
    write_wal_shipping_properties()
    write_wrapper_conf()
		write_monitor_checker_postgresql()
  end
  
  def write_replicator_properties
    # Configure replicator.properties - this sets up the plugin. 
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.properties.postgresql",
        "#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.properties", "# ")
    
    transformer.transform { |line|
      if line =~ /replicator.role=master/ then
        if is_master?()
          "replicator.role=master"
        else
          "replicator.role=slave"
        end
      elsif line =~ /replicator.master.uri/ then
        unless is_master?()
          "replicator.master.uri=wal://" + @config.getProperty(REPL_MASTERHOST) + "/"
        else
          "replicator.master.uri=wal://localhost/"
        end
      elsif line =~ /replicator.master.connect.uri/ then
        if is_master?() then
          line
        else
          "replicator.master.connect.uri=thl://" + @config.getProperty(REPL_MASTERHOST) + "/"
        end
      elsif line =~ /replicator.master.listen.uri/ then
        "replicator.master.listen.uri=thl://" + @config.getProperty(GLOBAL_HOST) + "/"
      elsif line =~ /replicator.auto_enable/ then
        "replicator.auto_enable=" + @config.getProperty(REPL_AUTOENABLE)
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /cluster.name/ then
        "cluster.name=" + @config.getProperty(GLOBAL_DSNAME)
      elsif line =~ /replicator.resourceJdbcUrl/
        line.sub("@HOSTNAME@", @config.getProperty(GLOBAL_HOST))
      elsif line =~ /replicator.backup.agents/
        if @config.getProperty(REPL_BACKUP_METHOD) == "none"
          "replicator.backup.agents="
        else
          "replicator.backup.agents=" + @config.getProperty(REPL_BACKUP_METHOD)
        end
      elsif line =~ /replicator.backup.default/
        if @config.getProperty(REPL_BACKUP_METHOD) == "none"
          "replicator.backup.default="
        else
          "replicator.backup.default=" + @config.getProperty(REPL_BACKUP_METHOD)
        end
      elsif line =~ /replicator.backup.agent.pg_dump.port/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.backup.agent.pg_dump.port=" + @config.getProperty(REPL_DBPORT)
      elsif line =~ /replicator.backup.agent.pg_dump.user/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.backup.agent.pg_dump.user=" + @config.getProperty(REPL_DBLOGIN)
      elsif line =~ /replicator.backup.agent.pg_dump.password/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.backup.agent.pg_dump.password=" + @config.getProperty(REPL_DBPASSWORD)
      elsif line =~ /replicator.backup.agent.pg_dump.dumpDir/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.backup.agent.pg_dump.dumpDir=" + @config.getProperty(REPL_BACKUP_DUMP_DIR)
      elsif line =~ /replicator.backup.agent.script.script/ && @config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.script=" + @config.getProperty(REPL_BACKUP_SCRIPT)
      elsif line =~ /replicator.backup.agent.script.commandPrefix/ && @config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.commandPrefix=" + @config.getProperty(REPL_BACKUP_COMMAND_PREFIX)
      elsif line =~ /replicator.backup.agent.script.hotBackupEnabled/ && @config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.hotBackupEnabled=" + @config.getProperty(REPL_BACKUP_ONLINE)
      elsif line =~ /replicator.storage.agents/
        if @config.getProperty(REPL_BACKUP_METHOD) == "none"
          "replicator.storage.agents="
        else
          "replicator.storage.agents=fs"
        end
      elsif line =~ /replicator.storage.agent.fs.directory/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.storage.agent.fs.directory=" + @config.getProperty(REPL_BACKUP_STORAGE_DIR)
      elsif line =~ /replicator.storage.agent.fs.retention/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
        "replicator.storage.agent.fs.retention=" + @config.getProperty(REPL_BACKUP_RETENTION)
      elsif line =~ /replicator.script.root/
        "replicator.script.root_dir=#{get_deployment_basedir()}/tungsten-replicator"
      elsif line =~ /replicator.script.conf_file/
        "replicator.script.conf_file=conf/postgresql-wal.properties"
      elsif line =~ /replicator.script.processor/
        "replicator.script.processor=bin/pg-wal-plugin"
      elsif line =~ /replicator.vipInterface/ && @config.getProperty(REPL_MASTER_VIP) != "none"
        "replicator.vipInterface=" + @config.getProperty(REPL_MASTER_VIP_DEVICE)
      elsif line =~ /replicator.vipAddress/ && @config.getProperty(REPL_MASTER_VIP) != "none"
        "replicator.vipAddress=" + @config.getProperty(REPL_MASTER_VIP)
      elsif line =~ /replicator.store.thl.log_file_retention/ && @config.getProperty(REPL_THL_LOG_RETENTION) != ""
        "replicator.store.thl.log_file_retention=#{@config.getProperty(REPL_THL_LOG_RETENTION)}"
      elsif line =~ /replicator.applier.consistency_policy/
        "replicator.applier.consistency_policy=#{@config.getProperty(REPL_CONSISTENCY_POLICY)}"
      else
        line
      end
    }
  end
  
  def write_wal_shipping_properties
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/conf/sample.postgresql-wal.properties",
        "#{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties", "# ")
    
    transformer.transform { |line|
      if line =~ /postgresql.streaming_replication/ then
        "postgresql.streaming_replication=" + @config.getProperty(REPL_PG_STREAMING)
      elsif line =~ /postgresql.data/ then
        "postgresql.data=" + @config.getProperty(REPL_PG_HOME)
      elsif line =~ /postgresql.conf/ then
        "postgresql.conf=" + @config.getProperty(REPL_PG_POSTGRESQL_CONF)
      elsif line =~ /^\s*postgresql.pg_standby=\s*(\S*)\s*$/ then
        # For pg_standby to work we need the full path of whatever is in 
        # the sample file. 
        pg_standby = which $1
        # If that does not work, try looking for the binaries under roo. 
        if ! pg_standby
          pg_standby = which(@config.getProperty(REPL_PG_ROOT) + "/bin/pg_standby")
        end
        if pg_standby
          "postgresql.pg_standby=" + pg_standby
        else
           raise RemoteError, "Unable to locate pg_standby; please ensure it is defined correctly in tungsten-replicator/conf/sample.postgresql-wal.properties" 
        end
      elsif line =~ /^\s*postgresql.pg_archivecleanup=\s*(\S*)\s*$/ then
        pg_archivecleanup = which $1
        if ! pg_archivecleanup
          pg_archivecleanup = which(@config.getProperty(REPL_PG_ROOT) + "/bin/pg_archivecleanup")
        end
        if pg_archivecleanup
          "postgresql.pg_archivecleanup=" + pg_archivecleanup
        elsif @config.getProperty(REPL_PG_STREAMING) == "true"
           raise RemoteError, "Unable to locate pg_archivecleanup; please ensure it is defined correctly in tungsten-replicator/conf/sample.postgresql-wal.properties" 
        end
      elsif line =~ /postgresql.archive_timeout/ then
        "postgresql.archive_timeout=" + @config.getProperty(REPL_PG_ARCHIVE_TIMEOUT)
      elsif line =~ /postgresql.archive/ then
        "postgresql.archive=" + @config.getProperty(REPL_PG_ARCHIVE)
      elsif line =~ /postgresql.role/ then
        if is_master?()
          "postgresql.role=master"
        else
          "postgresql.role=slave"
        end
      elsif line =~ /postgresql.master.host/ then
        if @config.getProperty(REPL_MASTERHOST)
          "postgresql.master.host=" + @config.getProperty(REPL_MASTERHOST)
        else
          "postgresql.master.host=" 
        end
      elsif line =~ /postgresql.master.user/ then
        "postgresql.master.user=" + @config.getProperty(REPL_DBLOGIN)
      elsif line =~ /postgresql.master.password/ then
        "postgresql.master.password=" + @config.getProperty(REPL_DBPASSWORD)
      elsif line =~ /postgresql.boot.script/ then
        "postgresql.boot.script=" + @config.getProperty(REPL_BOOT_SCRIPT)
      elsif line =~ /postgresql.root.prefix/ then
        "postgresql.root.prefix=" + get_root_prefix()
      else
        line
      end
    }
  end
  
  def write_wrapper_conf
    # Configure wrapper.conf to use the Open Replicator main class. 
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf",
        "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf", nil)
    
    transformer.transform { |line|
      if line =~ /wrapper.app.parameter.1/ then
        "wrapper.app.parameter.1=com.continuent.tungsten.replicator.management.OpenReplicatorManager"
      elsif line =~ /wrapper.java.maxmemory=/
        "wrapper.java.maxmemory=" + @config.getProperty(REPL_JAVA_MEM_SIZE)
      else
        line
      end
    }
  end
  
  def write_monitor_checker_postgresql
    # Configure monitoring for PostgreSQL. 
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-monitor/conf/sample.checker.postgresqlserver.properties",
        "#{get_deployment_basedir()}/tungsten-monitor/conf/checker.postgresqlserver.properties", "# ")
    
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    
    transformer.transform { |line|
      if line =~ /serverName=/
          "serverName=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /url=/ 
          "url=jdbc:postgresql://" + @config.getProperty(GLOBAL_HOST) + ':' + @config.getProperty(REPL_DBPORT) + "/" + user
      elsif line =~ /frequency=/
          "frequency=" + @config.getProperty(REPL_MONITOR_INTERVAL)
      elsif line =~ /host=/
          "host=" + @config.getProperty(GLOBAL_HOST)
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
  
  # Perform installation of required replication support files.  
  def postgresql_configuration
    # Select installation command. 
    info("Running procedure to configure warm standby...")
    cmd1 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg-wal-plugin -o uninstall -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    cmd2 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg-wal-plugin -o install -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    info("############ RESTART=#{@config.getProperty(GLOBAL_SVC_START)} ##################")
    
    if (@config.getProperty(GLOBAL_SVC_START) == "false")
      cmd2 = cmd2 + " -s"
    end
    
    Configurator.instance.write_divider()
    # Do uninstall first. 
    cmd_result(cmd1, true)
    # Then the install, which has to success. 
    cmd_result(cmd2, false)
    Configurator.instance.write_divider()
  end
end