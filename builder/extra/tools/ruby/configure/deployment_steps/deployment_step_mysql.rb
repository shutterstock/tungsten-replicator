module ConfigureDeploymentStepMySQL
  def transform_my_my_replication_dataservice_line(line, service_name, service_config)
		if line =~ /replicator.store.thl.url/ then
			if (service_config.getProperty(REPL_USE_DRIZZLE) == "true")
				"replicator.store.thl.url=jdbc:mysql:thin://" +
				  service_config.getProperty(REPL_DBHOST) + ":" +
				  service_config.getProperty(REPL_DBPORT) +
				  "/tungsten_${service.name}?createDB=true"
			else
				"replicator.store.thl.url=jdbc:mysql://" +
				  service_config.getProperty(REPL_DBHOST) + ":" +
				  service_config.getProperty(REPL_DBPORT) +
				  "/tungsten_${service.name}?createDatabaseIfNotExist=true"
			end
		elsif line =~ /replicator.extractor.mysql.binlog_dir/ then
			"replicator.extractor.mysql.binlog_dir=" + 
			  service_config.getProperty(REPL_MYSQL_BINLOGDIR)
		elsif line =~ /replicator.extractor.mysql.binlog_file_pattern/ then
			"replicator.extractor.mysql.binlog_file_pattern=" + 
			  service_config.getProperty(REPL_MYSQL_BINLOGPATTERN)
		elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/ && 
		    service_config.getProperty(REPL_BACKUP_METHOD) != "none"
			"replicator.backup.agent.mysqldump.dumpDir=" + 
			  service_config.getProperty(REPL_BACKUP_DUMP_DIR)
		elsif line =~ /replicator.extractor.mysql.usingBytesForString/
			"replicator.extractor.mysql.usingBytesForString=" + 
			  service_config.getProperty(REPL_USE_DRIZZLE)
		elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.useRelayLogs=" + 
			  service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS)
		elsif line =~ /replicator.extractor.mysql.relayLogDir/ && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.relayLogDir=" + 
			  service_config.getProperty(REPL_RELAY_LOG_DIR)
		elsif line =~ /replicator.extractor.mysql.relayLogRetention/ && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.relayLogRetention=3"
		elsif line =~ /replicator.extractor.mysql.serverId/ && 
		    service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
		  "replicator.extractor.mysql.serverId=#{service_config.getProperty(REPL_MYSQL_SERVER_ID)}"
		elsif line =~ /^replicator.backup.agent.xtrabackup.options/ && service_config.getPropertyOr(REPL_BACKUP_METHOD) == "xtrabackup"
      directory = service_config.getProperty(REPL_BACKUP_DUMP_DIR) + "/innobackup"
      unless mkdir_if_absent(directory)
        raise "Unable to create directory #{directory} for storing temporary Xtrabackup files"
      end
      
      archive = service_config.getProperty(REPL_BACKUP_DUMP_DIR) + "/innobackup.tar"
            
      "replicator.backup.agent.xtrabackup.options=" +
      "user=${replicator.global.db.user}&" +
      "password=${replicator.global.db.password}" + 
      "&host=#{service_config.getProperty(REPL_DBHOST)}" +
      "&port=#{service_config.getProperty(REPL_DBPORT)}" + 
      "&directory=#{directory}&archive=#{archive}" +
      "&mysqldatadir=#{service_config.getProperty(REPL_MYSQL_DATADIR)}" +
      "&mysql_service_command=#{service_config.getProperty(REPL_BOOT_SCRIPT)}"
		else
		  transform_replication_dataservice_line(line, service_name, service_config)
		end
	end
end