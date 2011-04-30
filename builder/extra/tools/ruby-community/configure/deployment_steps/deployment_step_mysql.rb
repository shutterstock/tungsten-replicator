system_require "configure/deployment_steps/deployment_step_replicator"
module ConfigureDeploymentStepMySQL
  include ConfigureDeploymentStepReplicator
  
  # The deploy_replicator method is defined in ConfigureDeploymentStepReplicator
  def get_deployment_methods
    unless Configurator.instance.package.is_a?(ConfigureServicePackage)
      [
        ConfigureDeploymentMethod.new("deploy_replicator"),
        ConfigureDeploymentMethod.new("deploy_replication_dataservices", 50)
      ]
    else
      [
      ]
    end
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
      "&mysqldatadir=#{service_config.getProperty(REPL_MYSQL_BINLOGDIR)}" +
      "&mysql_service_command=#{service_config.getProperty(REPL_BOOT_SCRIPT)}"
		else
		  super(line, service_name, service_config)
		end
	end
	
	def write_replication_service_properties
	  default_ds = @config.getProperty(DEFAULT_DATASERVER)
	  # Generate the services.properties file.
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.services.properties",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

    transformer.transform { |line|
      if line =~ /replicator.services/
        repl_services = []
        @config.getPropertyOr(REPL_SERVICES, {}).each{
          |service_alias,service_properties|
          repl_services << service_properties[DEPLOYMENT_SERVICE]
        }
        "replicator.services=" + repl_services.join(',')
      elsif line =~ /replicator.global.db.user=/ then
        "replicator.global.db.user=" + @config.getProperty([DATASERVERS, default_ds, REPL_DBLOGIN])
      elsif line =~ /replicator.global.db.password=/ then
        "replicator.global.db.password=" + @config.getProperty([DATASERVERS, default_ds, REPL_DBPASSWORD])
      elsif line =~ /replicator.resourceLogDir/ then
        "replicator.resourceLogDir=" + @config.getProperty([DATASERVERS, default_ds, REPL_MYSQL_BINLOGDIR])
      elsif line =~ /replicator.resourceLogPattern/ then
        "replicator.resourceLogPattern=" + @config.getProperty([DATASERVERS, default_ds, REPL_MYSQL_BINLOGPATTERN])
      elsif line =~ /replicator.resourceJdbcUrl/
        line = line.sub("$serviceFacet.name$", @config.getProperty([DATASERVERS, default_ds, REPL_DBHOST]) + 
          ":" + @config.getProperty([DATASERVERS, default_ds, REPL_DBPORT]))
      elsif line =~ /replicator.resourceDataServerHost/ then
        "replicator.resourceDataServerHost=" + @config.getProperty([DATASERVERS, default_ds, REPL_DBHOST])
      elsif line =~ /replicator.resourceJdbcDriver/ then
        "replicator.resourceJdbcDriver=com.mysql.jdbc.Driver"
      elsif line =~ /replicator.resourcePort/ then
        "replicator.resourcePort=" + @config.getProperty([DATASERVERS, default_ds, REPL_DBPORT])
      elsif line =~ /replicator.resourceDiskLogDir/ then
        "replicator.resourceDiskLogDir=" + @config.getProperty(REPL_LOG_DIR)
      elsif line =~ /replicator.source_id/ then
        "replicator.source_id=" + @config.getProperty(HOST)
      elsif line =~ /replicator.resourceVendor/ then
        "replicator.resourceVendor=" + @config.getProperty(DBMS_TYPE)
      elsif line =~ /cluster.name=/ then
        "cluster.name=" + @config.getPropertyOr(CLUSTERNAME, "")
      elsif line =~ /replicator.host=/ then
        "replicator.host=" + @config.getProperty(HOST)
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
  
  def get_replication_dataservice_template(service_config)
    if service_config.getProperty(REPL_USE_DRIZZLE) == "true"
			"#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql-with-drizzle-driver"
		else
		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.mysql"
		end
	end
end