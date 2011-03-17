module ConfigureDeploymentStepReplicationDataservice
  # The deploy_replicator method is defined in ConfigureDeploymentStepReplicator
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_replication_dataservice", 50)
    ]
  end
  module_function :get_deployment_methods
  
  def deploy_replication_dataservice
    mkdir_if_absent(@config.getProperty(REPL_LOG_DIR) + "/" + @config.getProperty(DSNAME))
    
    if @config.getProperty(REPL_RELAY_LOG_DIR)
      mkdir_if_absent(@config.getProperty(REPL_RELAY_LOG_DIR) + "/" + @config.getProperty(DSNAME))
    end
    
    # Configure replicator.properties.service.template
		transformer = Transformer.new(
		  get_mysql_replicator_properties_template(),
			"#{get_deployment_basedir()}/tungsten-replicator/conf/static-#{@config.getProperty(DSNAME)}.properties", "#")

		transformer.transform { |line|
		  if line =~ /replicator.role=/ then
        "replicator.role=" + get_replication_role()
		  elsif line =~ /replicator.global.db.host=/ then
        "replicator.global.db.host=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /replicator.service.type=/ then
        "replicator.service.type=" + get_replication_service_type()
      elsif line =~ /replicator.global.db.port=/ then
        "replicator.global.db.port=" + @config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.global.db.user=/ then
				"replicator.global.db.user=" + @config.getProperty(REPL_DBLOGIN)
			elsif line =~ /replicator.global.db.password=/ then
				"replicator.global.db.password=" + @config.getProperty(REPL_DBPASSWORD)
			elsif line =~ /replicator.auto_enable/ then
				"replicator.auto_enable=" + @config.getProperty(REPL_AUTOENABLE)
			elsif line =~ /replicator.source_id/ then
				"replicator.source_id=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /cluster.name=/ then
				"cluster.name=" + @config.getPropertyOr(GLOBAL_CLUSTERNAME, "")
			elsif line =~ /local.service.name=/ then
				"service.name=" + @config.getProperty(DSNAME)
			elsif line =~ /local.service.name=/ then
				"local.service.name=" + @config.getProperty(GLOBAL_DSNAME)
			elsif line =~ /replicator.service.type=/ then
        "replicator.service.type=local"
			elsif line =~ /replicator.global.buffer.size=/ then
				"replicator.global.buffer.size=" + @config.getProperty(REPL_BUFFER_SIZE)
			elsif line =~ /replicator.store.thl.url/ then
				if (@config.getProperty(REPL_USE_DRIZZLE) == "true")
					"replicator.store.thl.url=jdbc:mysql:thin://" +
					@config.getProperty(GLOBAL_HOST) + ":" +
					@config.getProperty(REPL_DBPORT) +
					"/tungsten_${service.name}?createDB=true"
				else
					"replicator.store.thl.url=jdbc:mysql://" +
					@config.getProperty(GLOBAL_HOST) + ":" +
					@config.getProperty(REPL_DBPORT) +
					"/tungsten_${service.name}?createDatabaseIfNotExist=true"
				end
			elsif line =~ /replicator.store.thl=/
				if @config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl=com.continuent.tungsten.enterprise.replicator.thl.EnterpriseTHL"
				else
					"replicator.store.thl=com.continuent.tungsten.replicator.thl.THL"
				end
			elsif line =~ /replicator.store.thl.storage=/
				if @config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl.storage=com.continuent.tungsten.enterprise.replicator.thl.DiskTHLStorage"
				else
					"replicator.store.thl.storage=com.continuent.tungsten.replicator.thl.JdbcTHLStorage"
				end
			elsif line =~ /replicator.store.thl.log_dir=/
				if @config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl.log_dir="+ @config.getProperty(REPL_LOG_DIR) + "/" + @config.getProperty(DSNAME)
				else
					"#" + line
				end
			elsif line =~ /replicator.master.listen.uri=/ then
				"replicator.master.listen.uri=thl://" + @config.getProperty(GLOBAL_HOST) + "/"
			elsif line =~ /replicator.extractor.mysql.binlog_dir/ then
				"replicator.extractor.mysql.binlog_dir=" + @config.getProperty(REPL_MYSQL_BINLOGDIR)
			elsif line =~ /replicator.extractor.mysql.host/ then
				"replicator.extractor.mysql.host=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.extractor.mysql.binlog_file_pattern/ then
				"replicator.extractor.mysql.binlog_file_pattern=" + @config.getProperty(REPL_MYSQL_BINLOGPATTERN)
			elsif line =~ /replicator.extractor.mysql.port/ then
				"replicator.extractor.mysql.port=" + @config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.applier.mysql.host/ then
				"replicator.applier.mysql.host=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.applier.mysql.port/ then
				"replicator.applier.mysql.port=" + @config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.resourceJdbcUrl/
				line = line.sub("@HOSTNAME@", @config.getProperty(GLOBAL_HOST) + ":" +
								@config.getProperty(REPL_DBPORT))
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
			elsif line =~ /replicator.backup.agent.mysqldump.host/
				"replicator.backup.agent.mysqldump.host=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.backup.agent.mysqldump.port/
				"replicator.backup.agent.mysqldump.port=" + @config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.backup.agent.mysqldump.dumpDir=" + @config.getProperty(REPL_BACKUP_DUMP_DIR)
			elsif line =~ /replicator.backup.agent.lvm.port/
				"replicator.backup.agent.lvm.port=" + @config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.backup.agent.lvm.host/
				"replicator.backup.agent.lvm.host=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.backup.agent.lvm.dumpDir/ && @config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.backup.agent.lvm.dumpDir=" + @config.getProperty(REPL_BACKUP_DUMP_DIR)
			elsif line =~ /replicator.backup.agent.lvm.dataDir/
				"replicator.backup.agent.lvm.dataDir=" + @config.getProperty(REPL_MYSQL_BINLOGDIR)
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
			elsif line =~ /replicator.extractor.mysql.usingBytesForString/
				"replicator.extractor.mysql.usingBytesForString=" + @config.getProperty(REPL_USE_BYTES)
			elsif line =~ /replicator.vipInterface/
				"replicator.vipInterface=" + @config.getPropertyOr(REPL_MASTER_VIP_DEVICE, "")
			elsif line =~ /replicator.vipAddress/
				"replicator.vipAddress=" + @config.getPropertyOr(REPL_MASTER_VIP, "")
			elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && @config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.useRelayLogs=true"
			elsif line =~ /replicator.extractor.mysql.relayLogDir/ && @config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.relayLogDir=" + @config.getProperty(REPL_RELAY_LOG_DIR) + "/" + @config.getProperty(DSNAME)
			elsif line =~ /replicator.extractor.mysql.relayLogRetention/ && @config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.relayLogRetention=3"
			elsif line =~ /replicator.store.thl.log_file_retention/ && @config.getProperty(REPL_THL_LOG_RETENTION) != ""
        "replicator.store.thl.log_file_retention=#{@config.getProperty(REPL_THL_LOG_RETENTION)}"
      elsif line =~ /replicator.applier.consistency_policy/
        "replicator.applier.consistency_policy=#{@config.getProperty(REPL_CONSISTENCY_POLICY)}"
      elsif line =~ /replicator.store.thl.doChecksum/
        "replicator.store.thl.doChecksum=#{@config.getProperty(REPL_THL_DO_CHECKSUM)}"
      elsif line =~ /replicator.store.thl.logConnectionTimeout/
        "replicator.store.thl.logConnectionTimeout=#{@config.getProperty(REPL_THL_LOG_CONNECTION_TIMEOUT)}"
      elsif line =~ /replicator.store.thl.log_file_size/
        "replicator.store.thl.log_file_size=#{@config.getProperty(REPL_THL_LOG_FILE_SIZE)}"
      elsif line =~ /replicator.extractor.mysql.binlogMode/ then
        "replicator.extractor.mysql.binlogMode=" + 
          @config.getProperty(REPL_SVC_BINLOG_MODE)
      elsif line =~ /replicator.master.connect.uri=/ then
        "replicator.master.connect.uri=thl://" + 
          @config.getProperty(REPL_MASTERHOST) + ":" + 
          @config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.master.listen.uri=/ then
        "replicator.master.listen.uri=thl://" + 
          @config.getProperty(GLOBAL_HOST) + ":" + 
          @config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.store.thl.storageListenerUri=/ then
        "replicator.store.thl.storageListenerUri=thl://0.0.0.0:" + 
          @config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.global.apply.channels=/
        "replicator.global.apply.channels=" + @config.getProperty(REPL_SVC_CHANNELS)
			else
			  line
			end
		}
  end
end