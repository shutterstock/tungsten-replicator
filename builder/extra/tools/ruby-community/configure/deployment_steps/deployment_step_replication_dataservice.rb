module ConfigureDeploymentStepReplicationDataservice
  def get_deployment_methods
    [
    ]
  end
  module_function :get_deployment_methods
  
  def deploy_replication_dataservices
    @config.getPropertyOr(REPL_SERVICES, {}).each{
      |service_alias,service_properties|
      
      service_config = Properties.new()
      service_config.props = @config.props.merge(service_properties)
      service_config.setProperty(REPL_SERVICES, nil)
      
      deploy_replication_dataservice(service_properties[DEPLOYMENT_SERVICE], service_config)
    }
  end
  
  def deploy_replication_dataservice(service_name, service_config)
    mkdir_if_absent(service_config.getProperty(REPL_LOG_DIR))
    
    if service_config.getProperty(REPL_RELAY_LOG_DIR)
      mkdir_if_absent(service_config.getProperty(REPL_RELAY_LOG_DIR))
    end
    
    # Configure replicator.properties.service.template
		transformer = Transformer.new(
		  get_replication_dataservice_template(service_config),
			service_config.getProperty(REPL_SVC_CONFIG_FILE), "#")
			
		case service_config.getProperty([DATASERVERS, service_config.getProperty(REPL_DATASERVER), DBMS_TYPE])
		when "mysql"
		  transform_func = lambda do |line, service_name, service_config|
		    transform_my_my_replication_dataservice_line(line, service_name, service_config)
		  end
		when "postgresql"
		  transform_func = lambda do |line, service_name, service_config|
		    transform_pg_pg_replication_dataservice_line(line, service_name, service_config)
		  end
		end
		
		transformer.transform { |line|
		  transform_func.call(line, service_name, service_config)
		}
		
		if service_config.getProperty(REPL_SVC_START) == "true"
		  if svc_is_running?(get_svc_command("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator"))
		    begin
		      info("Check if the replication service is running")
		      cmd_result("#{get_trepctl_cmd()} -service #{service_config.getProperty(DEPLOYMENT_SERVICE)} status")
		      warn("The replication service is already running.  You must stop and start it for the changes to take effect.")
        rescue CommandError => ce
          # The status command fails if the service is not running
          info("Start the replication service")
          cmd_result("#{get_trepctl_cmd()} -service #{service_config.getProperty(DEPLOYMENT_SERVICE)} start")
        end
      else
        info("Start the replicator")
        cmd_result(get_svc_command("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator start"))
      end
    end
  end
  
  def transform_replication_dataservice_line(line, service_name, service_config)
    if line =~ /replicator.role=/ then
      "replicator.role=" + service_config.getProperty(REPL_ROLE)
	  elsif line =~ /replicator.service.type=/ then
      "replicator.service.type=" + service_config.getProperty(REPL_SVC_SERVICE_TYPE)
    elsif line =~ /replicator.global.db.host=/ then
      "replicator.global.db.host=" + service_config.getProperty(REPL_DBHOST)
    elsif line =~ /replicator.global.db.port=/ then
      "replicator.global.db.port=" + service_config.getProperty(REPL_DBPORT)
		elsif line =~ /replicator.global.db.user=/ then
			"replicator.global.db.user=" + service_config.getProperty(REPL_DBLOGIN)
		elsif line =~ /replicator.global.db.password=/ then
			"replicator.global.db.password=" + service_config.getProperty(REPL_DBPASSWORD)
		elsif line =~ /replicator.global.extract.db.host=/ &&
        service_config.getProperty(REPL_EXTRACTOR_DBHOST) then
      "replicator.global.extract.db.host=" + service_config.getProperty(REPL_EXTRACTOR_DBHOST)
    elsif line =~ /replicator.global.extract.db.port=/ &&
        service_config.getProperty(REPL_EXTRACTOR_DBPORT) then
      "replicator.global.extract.db.port=" + service_config.getProperty(REPL_EXTRACTOR_DBPORT)
		elsif line =~ /replicator.global.extract.db.user=/ &&
        service_config.getProperty(REPL_EXTRACTOR_DBLOGIN) then
			"replicator.global.extract.db.user=" + service_config.getProperty(REPL_EXTRACTOR_DBLOGIN)
		elsif line =~ /replicator.global.extract.db.password=/ &&
        service_config.getProperty(REPL_EXTRACTOR_DBPASSWORD) then
			"replicator.global.extract.db.password=" + service_config.getProperty(REPL_EXTRACTOR_DBPASSWORD)
		elsif line =~ /replicator.auto_enable/ then
			"replicator.auto_enable=" + service_config.getProperty(REPL_AUTOENABLE)
		elsif line =~ /replicator.source_id/ then
			"replicator.source_id=" + service_config.getProperty(REPL_DBHOST)
		elsif line =~ /cluster.name=/ then
			"cluster.name=" + @config.getProperty(CLUSTERNAME)
		elsif line =~ /^service.name=/ then
			"service.name=" + service_name
		elsif line =~ /^local.service.name=/ then
			"local.service.name=" + service_config.getProperty(DSNAME)
		elsif line =~ /replicator.service.type=/ then
      "replicator.service.type=local"
		elsif line =~ /replicator.global.buffer.size=/ then
			"replicator.global.buffer.size=" + service_config.getProperty(REPL_BUFFER_SIZE)
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
				"replicator.store.thl.log_dir="+ service_config.getProperty(REPL_LOG_DIR)
			else
				"#" + line
			end
		elsif line =~ /replicator.master.listen.uri=/ then
			"replicator.master.listen.uri=thl://" + service_config.getProperty(HOST) + ":" + 
			  service_config.getProperty(REPL_SVC_THL_PORT) + "/"
		elsif line =~ /replicator.resourceJdbcUrl/
			line = line.sub("@HOSTNAME@", service_config.getProperty(REPL_DBHOST) + ":" +
				service_config.getProperty(REPL_DBPORT))
		elsif line =~ /replicator.backup.agents/
			if service_config.getProperty(REPL_BACKUP_METHOD) == "none"
				"replicator.backup.agents="
			else
				"replicator.backup.agents=" + service_config.getPropertyOr(REPL_BACKUP_METHOD, "")
			end
		elsif line =~ /replicator.backup.default/
			if service_config.getProperty(REPL_BACKUP_METHOD) == "none"
				"replicator.backup.default="
			else
				"replicator.backup.default=" + service_config.getPropertyOr(REPL_BACKUP_METHOD, "")
			end
		elsif line =~ /replicator.backup.agent.lvm.port/
			"replicator.backup.agent.lvm.port=" + service_config.getProperty(REPL_DBPORT)
		elsif line =~ /replicator.backup.agent.lvm.host/
			"replicator.backup.agent.lvm.host=" + service_config.getProperty(REPL_DBHOST)
		elsif line =~ /replicator.backup.agent.lvm.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
			"replicator.backup.agent.lvm.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
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
		elsif line =~ /replicator.vipInterface/
			"replicator.vipInterface=" + service_config.getPropertyOr(REPL_MASTER_VIP_DEVICE, "")
		elsif line =~ /replicator.vipAddress/
			"replicator.vipAddress=" + service_config.getPropertyOr(REPL_MASTER_VIP, "")
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
    elsif line =~ /replicator.master.connect.uri=/  && 
        service_config.getProperty(REPL_ROLE) == REPL_ROLE_S then
      "replicator.master.connect.uri=thl://" + 
        service_config.getProperty(REPL_MASTERHOST) + ":" + 
        service_config.getProperty(REPL_MASTERPORT) + "/"
    elsif line =~ /replicator.master.listen.uri=/ then
      "replicator.master.listen.uri=thl://" + 
        service_config.getProperty(HOST) + ":" + 
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
    elsif line =~ /replicator.rmi_port=/ then
      "replicator.rmi_port=" + service_config.getProperty(REPL_RMI_PORT)
		else
		  line
		end
	end
	
	def get_replication_dataservice_template(service_config)
    dbms_type = service_config.getProperty([DATASERVERS, service_config.getProperty(REPL_DATASERVER), DBMS_TYPE])
    
    case dbms_type
    when "mysql"
      if service_config.getProperty(REPL_USE_DRIZZLE) == "true"
  			"#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql-with-drizzle-driver"
  		else
  		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql"
  		end
  	when "postgresql"
  	  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.static.properties.postgresql"
  	else
  	  error("Unable to determine the replicator properties template for #{dbms_type}")
  	end
	end
end