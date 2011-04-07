module ConfigureDeploymentStepReplicationDataservice
  # The deploy_replicator method is defined in ConfigureDeploymentStepReplicator
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_replication_dataservices", 50)
    ]
  end
  module_function :get_deployment_methods
  
  def deploy_replication_dataservices
    @config.getProperty(REPL_SERVICES).split(",").each{
      |service_name|
      service_properties = @config.getProperty(Configurator::SERVICE_CONFIG_PREFIX + service_name)
      
      if service_properties[REPL_MASTERHOST] == @config.getProperty(GLOBAL_HOST)
        @config.setProperty(GLOBAL_DSNAME, service_name)
      end
    }
    
    @config.getProperty(REPL_SERVICES).split(",").each{
      |service_name|
      service_properties = @config.getProperty(Configurator::SERVICE_CONFIG_PREFIX + service_name)
      
      unless service_properties
        raise "Unable to find service configuration for '#{service_name}'"
      end
      
      service_config = @config.dup()
      service_properties.each {
        |key,value|
        service_config.setProperty(key, value)
      }
      
      unless service_config.getProperty(REPL_HOSTS)
        raise "Missing replication hosts definition for '#{service_name}' configuration"
      end
      
      service_hosts = service_config.getProperty(REPL_HOSTS).split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        deploy_replication_dataservice(service_name, service_config)
      end
    }
  end
  
  def deploy_replication_dataservice(service_name, service_config)
    mkdir_if_absent(service_config.getProperty(REPL_LOG_DIR) + "/" + service_name)
    
    if service_config.getProperty(REPL_RELAY_LOG_DIR)
      mkdir_if_absent(service_config.getProperty(REPL_RELAY_LOG_DIR) + "/" + service_name)
    end
    
    if service_config.getProperty(REPL_MASTERHOST) == service_config.getProperty(GLOBAL_HOST)
      role = "master"
    else
      role = "slave"
    end

    if service_config.getProperty(REPL_REMOTE_HOSTS) && service_config.getProperty(REPL_REMOTE_HOSTS).split(",").include?(service_config.getProperty(GLOBAL_HOST))
      service_type = 'remote'
    else
      service_type = 'local'
    end
    
    # Configure replicator.properties.service.template
		transformer = Transformer.new(
		  get_mysql_replicator_properties_template(),
			"#{get_deployment_basedir()}/tungsten-replicator/conf/static-#{service_name}.properties", "#")

		transformer.transform { |line|
		  if line =~ /replicator.role=/ then
        "replicator.role=" + role
		  elsif line =~ /replicator.global.db.host=/ then
        "replicator.global.db.host=" + service_config.getProperty(GLOBAL_HOST)
      elsif line =~ /replicator.service.type=/ then
        "replicator.service.type=" + service_type
      elsif line =~ /replicator.global.db.port=/ then
        "replicator.global.db.port=" + service_config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.global.db.user=/ then
				"replicator.global.db.user=" + service_config.getProperty(REPL_DBLOGIN)
			elsif line =~ /replicator.global.db.password=/ then
				"replicator.global.db.password=" + service_config.getProperty(REPL_DBPASSWORD)
			elsif line =~ /replicator.auto_enable/ then
				"replicator.auto_enable=" + service_config.getProperty(REPL_AUTOENABLE)
			elsif line =~ /replicator.source_id/ then
				"replicator.source_id=" + service_config.getProperty(GLOBAL_HOST)
			elsif line =~ /cluster.name=/ then
				"cluster.name=" + service_config.getPropertyOr(GLOBAL_CLUSTERNAME, "")
			elsif line =~ /^service.name=/ then
				"service.name=" + service_name
			elsif line =~ /^local.service.name=/ then
				"local.service.name=" + service_config.getProperty(GLOBAL_DSNAME)
			elsif line =~ /replicator.service.type=/ then
        "replicator.service.type=local"
			elsif line =~ /replicator.global.buffer.size=/ then
				"replicator.global.buffer.size=" + service_config.getProperty(REPL_BUFFER_SIZE)
			elsif line =~ /replicator.store.thl.url/ then
				if (service_config.getProperty(REPL_USE_DRIZZLE) == "true")
					"replicator.store.thl.url=jdbc:mysql:thin://" +
					service_config.getProperty(GLOBAL_HOST) + ":" +
					service_config.getProperty(REPL_DBPORT) +
					"/tungsten_${service.name}?createDB=true"
				else
					"replicator.store.thl.url=jdbc:mysql://" +
					service_config.getProperty(GLOBAL_HOST) + ":" +
					service_config.getProperty(REPL_DBPORT) +
					"/tungsten_${service.name}?createDatabaseIfNotExist=true"
				end
			elsif line =~ /replicator.store.thl=/
				if service_config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl=com.continuent.tungsten.enterprise.replicator.thl.EnterpriseTHL"
				else
					"replicator.store.thl=com.continuent.tungsten.replicator.thl.THL"
				end
			elsif line =~ /replicator.store.thl.storage=/
				if service_config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl.storage=com.continuent.tungsten.enterprise.replicator.thl.DiskTHLStorage"
				else
					"replicator.store.thl.storage=com.continuent.tungsten.replicator.thl.JdbcTHLStorage"
				end
			elsif line =~ /replicator.store.thl.log_dir=/
				if service_config.getProperty(REPL_LOG_TYPE) == "disk" then
					"replicator.store.thl.log_dir="+ service_config.getProperty(REPL_LOG_DIR) + "/" + service_name
				else
					"#" + line
				end
			elsif line =~ /replicator.master.listen.uri=/ then
				"replicator.master.listen.uri=thl://" + service_config.getProperty(GLOBAL_HOST) + "/"
			elsif line =~ /replicator.extractor.mysql.binlog_dir/ then
				"replicator.extractor.mysql.binlog_dir=" + service_config.getProperty(REPL_MYSQL_BINLOGDIR)
			elsif line =~ /replicator.extractor.mysql.host/ then
				"replicator.extractor.mysql.host=" + service_config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.extractor.mysql.binlog_file_pattern/ then
				"replicator.extractor.mysql.binlog_file_pattern=" + service_config.getProperty(REPL_MYSQL_BINLOGPATTERN)
			elsif line =~ /replicator.extractor.mysql.port/ then
				"replicator.extractor.mysql.port=" + service_config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.applier.mysql.host/ then
				"replicator.applier.mysql.host=" + service_config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.applier.mysql.port/ then
				"replicator.applier.mysql.port=" + service_config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.resourceJdbcUrl/
				line = line.sub("@HOSTNAME@", service_config.getProperty(GLOBAL_HOST) + ":" +
								service_config.getProperty(REPL_DBPORT))
			elsif line =~ /replicator.backup.agents/
				if service_config.getProperty(REPL_BACKUP_METHOD) == "none"
					"replicator.backup.agents="
				else
					"replicator.backup.agents=" + service_config.getProperty(REPL_BACKUP_METHOD)
				end
			elsif line =~ /replicator.backup.default/
				if service_config.getProperty(REPL_BACKUP_METHOD) == "none"
					"replicator.backup.default="
				else
					"replicator.backup.default=" + service_config.getProperty(REPL_BACKUP_METHOD)
				end
			elsif line =~ /replicator.backup.agent.mysqldump.host/
				"replicator.backup.agent.mysqldump.host=" + service_config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.backup.agent.mysqldump.port/
				"replicator.backup.agent.mysqldump.port=" + service_config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.backup.agent.mysqldump.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
			elsif line =~ /replicator.backup.agent.lvm.port/
				"replicator.backup.agent.lvm.port=" + service_config.getProperty(REPL_DBPORT)
			elsif line =~ /replicator.backup.agent.lvm.host/
				"replicator.backup.agent.lvm.host=" + service_config.getProperty(GLOBAL_HOST)
			elsif line =~ /replicator.backup.agent.lvm.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.backup.agent.lvm.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
			elsif line =~ /replicator.backup.agent.lvm.dataDir/
				"replicator.backup.agent.lvm.dataDir=" + service_config.getProperty(REPL_MYSQL_BINLOGDIR)
			elsif line =~ /replicator.backup.agent.script.script/ && service_config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.script=" + service_config.getProperty(REPL_BACKUP_SCRIPT)
      elsif line =~ /replicator.backup.agent.script.commandPrefix/ && service_config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.commandPrefix=" + service_config.getProperty(REPL_BACKUP_COMMAND_PREFIX)
      elsif line =~ /replicator.backup.agent.script.hotBackupEnabled/ && service_config.getProperty(REPL_BACKUP_METHOD) == "script"
        "replicator.backup.agent.script.hotBackupEnabled=" + service_config.getProperty(REPL_BACKUP_ONLINE)
			elsif line =~ /replicator.storage.agents/
				if service_config.getProperty(REPL_BACKUP_METHOD) == "none"
					"replicator.storage.agents="
				else
					"replicator.storage.agents=fs"
				end
			elsif line =~ /replicator.storage.agent.fs.directory/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.storage.agent.fs.directory=" + service_config.getProperty(REPL_BACKUP_STORAGE_DIR)
			elsif line =~ /replicator.storage.agent.fs.retention/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
				"replicator.storage.agent.fs.retention=" + service_config.getProperty(REPL_BACKUP_RETENTION)
			elsif line =~ /replicator.extractor.mysql.usingBytesForString/
				"replicator.extractor.mysql.usingBytesForString=" + service_config.getProperty(REPL_USE_BYTES)
			elsif line =~ /replicator.vipInterface/
				"replicator.vipInterface=" + service_config.getPropertyOr(REPL_MASTER_VIP_DEVICE, "")
			elsif line =~ /replicator.vipAddress/
				"replicator.vipAddress=" + service_config.getPropertyOr(REPL_MASTER_VIP, "")
			elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.useRelayLogs=true"
			elsif line =~ /replicator.extractor.mysql.relayLogDir/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.relayLogDir=" + service_config.getProperty(REPL_RELAY_LOG_DIR) + "/" + service_name
			elsif line =~ /replicator.extractor.mysql.relayLogRetention/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
				"replicator.extractor.mysql.relayLogRetention=3"
			elsif line =~ /replicator.store.thl.log_file_retention/ && service_config.getProperty(REPL_THL_LOG_RETENTION) != ""
        "replicator.store.thl.log_file_retention=#{service_config.getProperty(REPL_THL_LOG_RETENTION)}"
      elsif line =~ /replicator.applier.consistency_policy/
        "replicator.applier.consistency_policy=#{service_config.getProperty(REPL_CONSISTENCY_POLICY)}"
      elsif line =~ /replicator.store.thl.doChecksum/
        "replicator.store.thl.doChecksum=#{service_config.getProperty(REPL_THL_DO_CHECKSUM)}"
      elsif line =~ /replicator.store.thl.logConnectionTimeout/
        "replicator.store.thl.logConnectionTimeout=#{service_config.getProperty(REPL_THL_LOG_CONNECTION_TIMEOUT)}"
      elsif line =~ /replicator.store.thl.log_file_size/
        "replicator.store.thl.log_file_size=#{service_config.getProperty(REPL_THL_LOG_FILE_SIZE)}"
      elsif line =~ /replicator.extractor.mysql.binlogMode/ then
        "replicator.extractor.mysql.binlogMode=" + 
          service_config.getProperty(REPL_SVC_BINLOG_MODE)
      elsif line =~ /replicator.master.connect.uri=/ then
        "replicator.master.connect.uri=thl://" + 
          service_config.getProperty(REPL_MASTERHOST) + ":" + 
          service_config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.master.listen.uri=/ then
        "replicator.master.listen.uri=thl://" + 
          service_config.getProperty(GLOBAL_HOST) + ":" + 
          service_config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.store.thl.storageListenerUri=/ then
        "replicator.store.thl.storageListenerUri=thl://0.0.0.0:" + 
          service_config.getProperty(REPL_SVC_THL_PORT) + "/"
      elsif line =~ /replicator.global.apply.channels=/
        "replicator.global.apply.channels=" + service_config.getProperty(REPL_SVC_CHANNELS)
      elsif line =~ /replicator.shard.default.db=/
        "replicator.shard.default.db=" + service_config.getProperty(REPL_SVC_SHARD_DEFAULT_DB)
      elsif line =~ /replicator.filter.bidiSlave.allowBidiUnsafe=/
        "replicator.filter.bidiSlave.allowBidiUnsafe=" + service_config.getProperty(REPL_SVC_ALLOW_BIDI_UNSAFE)
      elsif line =~ /replicator.filter.bidiSlave.allowAnyRemoteService=/
        "replicator.filter.bidiSlave.allowAnyRemoteService=" + service_config.getProperty(REPL_SVC_ALLOW_ANY_SERVICE)
			else
			  line
			end
		}
  end
end