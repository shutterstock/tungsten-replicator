module ConfigureDeploymentStepPostgresql
  def apply_config_replicator
    write_wal_shipping_properties()
  end
  
  def transform_pg_pg_replication_dataservice_line(line, service_name, service_config)
    if line =~ /replicator.master.uri/ then
      if service_config.getProperty(REPL_MASTERHOST)
        "replicator.master.uri=wal://" + service_config.getProperty(REPL_MASTERHOST) + "/"
      else
        "replicator.master.uri=wal://localhost/"
      end
    elsif line =~ /replicator.master.connect.uri/ then
      if service_config.getProperty(REPL_ROLE) == "master" then
        line
      else
        "replicator.master.connect.uri=thl://" + service_config.getProperty(REPL_MASTERHOST) + "/"
      end
    elsif line =~ /replicator.script.root/
      "replicator.script.root_dir=" + File.expand_path("#{get_deployment_basedir()}/tungsten-replicator")
    elsif line =~ /replicator.script.conf_file/
      "replicator.script.conf_file=conf/postgresql-wal.properties"
    elsif line =~ /replicator.script.processor/
      "replicator.script.processor=bin/pg/pg-wal-plugin"
    elsif line =~ /replicator.backup.agent.pg_dump.port/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.port=" + service_config.getProperty(REPL_DBPORT)
    elsif line =~ /replicator.backup.agent.pg_dump.user/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.user=" + service_config.getProperty(REPL_DBLOGIN)
    elsif line =~ /replicator.backup.agent.pg_dump.password/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.password=" + service_config.getProperty(REPL_DBPASSWORD)
    elsif line =~ /replicator.backup.agent.pg_dump.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) == "pg_dump"
      "replicator.backup.agent.pg_dump.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
		else
		  transform_replication_dataservice_line(line, service_name, service_config)
		end
	end
  
  def write_wal_shipping_properties
    transformer = Transformer.new(
        "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.postgresql-wal.properties",
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
           raise RemoteError, "Unable to locate pg_standby; please ensure it is defined correctly in tungsten-replicator/conf/postgresql-wal.properties" 
        end
      elsif line =~ /^\s*postgresql.pg_archivecleanup=\s*(\S*)\s*$/ then
        pg_archivecleanup = which $1
        if ! pg_archivecleanup
          pg_archivecleanup = which(@config.getProperty(REPL_PG_ROOT) + "/bin/pg_archivecleanup")
        end
        if pg_archivecleanup
          "postgresql.pg_archivecleanup=" + pg_archivecleanup
        elsif @config.getProperty(REPL_PG_STREAMING) == "true"
           raise RemoteError, "Unable to locate pg_archivecleanup; please ensure it is defined correctly in tungsten-replicator/conf/postgresql-wal.properties" 
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
      elsif line =~ /postgresql.master.host/ && service_properties[REPL_ROLE] == REPL_ROLE_M then
        "postgresql.master.host=" + service_properties[REPL_MASTERHOST]
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
  
  # Perform installation of required replication support files.  
  def postgresql_configuration
    # Select installation command. 
    info("Running procedure to configure warm standby...")
    cmd1 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg/pg-wal-plugin -o uninstall -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    cmd2 = "#{get_deployment_basedir()}/tungsten-replicator/bin/pg/pg-wal-plugin -o install -c #{get_deployment_basedir()}/tungsten-replicator/conf/postgresql-wal.properties"
    info("############ RESTART=#{@config.getProperty(SVC_START)} ##################")
    
    if (@config.getProperty(SVC_START) == "false")
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