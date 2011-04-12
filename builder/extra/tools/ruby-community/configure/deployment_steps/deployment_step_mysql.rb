system_require "configure/deployment_steps/deployment_step_replicator"
module ConfigureDeploymentStepMySQL
  include ConfigureDeploymentStepReplicator
  
  # The deploy_replicator method is defined in ConfigureDeploymentStepReplicator
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_replicator")
    ]
  end
  module_function :get_deployment_methods
  
  def apply_config_replicator
    write_replication_service_properties()
		write_replicator_thl()
		write_wrapper_conf()
		deploy_mysql_connectorj_package()
		write_replicator_dynamic_properties()
  end
  
  def transform_replication_dataservice_line(line, service_name, service_config)
		if line =~ /replicator.store.thl.url/ then
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
		elsif line =~ /replicator.backup.agent.mysqldump.host/
			"replicator.backup.agent.mysqldump.host=" + service_config.getProperty(GLOBAL_HOST)
		elsif line =~ /replicator.backup.agent.mysqldump.port/
			"replicator.backup.agent.mysqldump.port=" + service_config.getProperty(REPL_DBPORT)
		elsif line =~ /replicator.backup.agent.mysqldump.dumpDir/ && service_config.getProperty(REPL_BACKUP_METHOD) != "none"
			"replicator.backup.agent.mysqldump.dumpDir=" + service_config.getProperty(REPL_BACKUP_DUMP_DIR)
		elsif line =~ /replicator.extractor.mysql.usingBytesForString/
			"replicator.extractor.mysql.usingBytesForString=" + service_config.getProperty(REPL_USE_BYTES)
		elsif line =~ /replicator.extractor.mysql.useRelayLogs/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.useRelayLogs=true"
		elsif line =~ /replicator.extractor.mysql.relayLogDir/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.relayLogDir=" + service_config.getProperty(REPL_RELAY_LOG_DIR)
		elsif line =~ /replicator.extractor.mysql.relayLogRetention/ && service_config.getProperty(REPL_EXTRACTOR_USE_RELAY_LOGS) == "true"
			"replicator.extractor.mysql.relayLogRetention=3"
    elsif line =~ /replicator.extractor.mysql.binlogMode/ then
      "replicator.extractor.mysql.binlogMode=" + 
        service_config.getProperty(REPL_SVC_BINLOG_MODE)
		else
		  super(line, service_name, service_config)
		end
	end
	
	def write_replication_service_properties
	  # Generate the services.properties file.
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/conf/sample.services.properties",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

    transformer.transform { |line|
      if line =~ /replicator.services/
        "replicator.services=" + @config.getProperty(REPL_SERVICES)
      elsif line =~ /replicator.global.db.user=/ then
        "replicator.global.db.user=" + @config.getProperty(REPL_DBLOGIN)
      elsif line =~ /replicator.global.db.password=/ then
        "replicator.global.db.password=" + @config.getProperty(REPL_DBPASSWORD)
      elsif line =~ /replicator.resourceLogDir/ then
        "replicator.resourceLogDir=" + @config.getProperty(REPL_MYSQL_BINLOGDIR)
      elsif line =~ /replicator.resourceLogPattern/ then
        "replicator.resourceLogPattern=" + @config.getProperty(REPL_MYSQL_BINLOGPATTERN)
      elsif line =~ /replicator.resourceJdbcUrl/
        line = line.sub("$serviceFacet.name$", @config.getProperty(GLOBAL_HOST) + ":" +
          @config.getProperty(REPL_DBPORT))
      elsif line =~ /replicator.resourceDataServerHost/ then
        "replicator.resourceDataServerHost=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /replicator.resourceJdbcDriver/ then
        "replicator.resourceJdbcDriver=com.mysql.jdbc.Driver"
      elsif line =~ /replicator.resourcePort/ then
        "replicator.resourcePort=" + @config.getProperty(REPL_DBPORT)
      elsif line =~ /replicator.resourceDiskLogDir/ then
        "replicator.resourceDiskLogDir=" + @config.getProperty(REPL_LOG_DIR)
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /replicator.resourceVendor/ then
        "replicator.resourceVendor=" + @config.getProperty(GLOBAL_DBMS_TYPE)
      elsif line =~ /cluster.name=/ then
        "cluster.name=" + @config.getPropertyOr(GLOBAL_CLUSTERNAME, "")
      elsif line =~ /replicator.host=/ then
        "replicator.host=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /replicator.rmi_port=/ then
        "replicator.rmi_port=" + @config.getProperty(REPL_RMI_PORT)
      else
        line
      end
    }
  end
  
  def write_replicator_thl
		# Fix up the THL utility class name.
		transformer = Transformer.new(
									  "#{get_deployment_basedir()}/tungsten-replicator/bin/thl",
									  "#{get_deployment_basedir()}/tungsten-replicator/bin/thl", nil)
	
		transformer.transform { |line|
			if line =~ /THL_CTRL=/
				unless Configurator.instance.is_enterprise?()
					"THL_CTRL=com.continuent.tungsten.replicator.thl.THLManagerCtrl"
				else
					"THL_CTRL=com.continuent.tungsten.enterprise.replicator.thl.EnterpriseTHLManagerCtrl"
				end
				else
				line
			end
		}
	end
  
  def deploy_mysql_connectorj_package
		connector = @config.getProperty(REPL_MYSQL_CONNECTOR_PATH)
		if connector != nil and connector != "" and File.exist?(connector)
		  # Deploy user's specified MySQL Connector/J (TENT-222).
  		info "Deploying MySQL Connector/J..."
			FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-replicator/lib/")
		end
	end
	
	def write_replicator_dynamic_properties
	  # Configure auto-provisioning
		dynamic_properties = Properties.new
		if File.exist?(get_dynamic_properties_file())
			dynamic_properties.load(get_dynamic_properties_file())
		end
		dynamic_properties.setProperty("replicator.auto_provision", @config.getProperty(REPL_BACKUP_AUTO_PROVISION))
		dynamic_properties.setProperty("replicator.auto_backup", @config.getProperty(REPL_BACKUP_AUTO_BACKUP))
		dynamic_properties.store(get_dynamic_properties_file(), false)
	end
	
	def write_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.java.maxmemory=/
        "wrapper.java.maxmemory=" + @config.getProperty(REPL_JAVA_MEM_SIZE)
      else
        line
      end
    }
  end
  
  def get_replication_dataservice_template
    if @config.getProperty(REPL_USE_DRIZZLE) == "true"
			"#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.properties.mysql-with-drizzle-driver"
		else
		  "#{get_deployment_basedir()}/tungsten-replicator/conf/replicator.properties.mysql"
		end
	end
end