module ConfigureDeploymentStepReplicator
  def get_deployment_methods
    unless Configurator.instance.package.is_a?(ConfigureServicePackage)
      [
        ConfigureDeploymentMethod.new("deploy_replicator"),
        ConfigureDeploymentMethod.new("deploy_replication_dataservices", 50)
#        ConfigureDeploymentMethod.new("postgresql_configuration", ConfigureDeployment::FINAL_STEP_WEIGHT-1)
      ]
    else
      []
    end
  end
  module_function :get_deployment_methods
  
  def deploy_replicator
    unless is_replicator?()
      info("Tungsten Replicator is not active; skipping configuration")
      return
    end
    
    Configurator.instance.write_header("Perform Tungsten Replicator configuration")    
    
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_DUMP_DIR))
    mkdir_if_absent(@config.getProperty(REPL_BACKUP_STORAGE_DIR))
    mkdir_if_absent(@config.getProperty(REPL_RELAY_LOG_DIR))
    mkdir_if_absent(@config.getProperty(REPL_LOG_DIR))

    write_replication_service_properties()
    write_wrapper_conf()
    deploy_mysql_connectorj_package()
    add_service("tungsten-replicator/bin/replicator")
    set_run_as_user("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator")
  end
  
  def deploy_replication_dataservices
    @config.getPropertyOr(REPL_SERVICES, {}).each{
      |service_alias,service_properties|
      
      @config.setProperty(DEPLOYMENT_SERVICE, service_alias)
      
      deploy_replication_dataservice()
        
      @config.setProperty(DEPLOYMENT_SERVICE, nil)
    }
  end
  
  def write_replication_service_properties
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.services.properties",
			"#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

	  transformer.transform_values(method(:transform_values))

    transformer.output
  end
	
	def write_wrapper_conf
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf",
      "#{get_deployment_basedir()}/tungsten-replicator/conf/wrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.java.maxmemory=/
        "wrapper.java.maxmemory=" + @config.getProperty(REPL_JAVA_MEM_SIZE)
      elsif line =~ /jolokia-jvm/
        if @config.getProperty(REPL_API) == "true"
          if line[0,1] == "#"
            line.slice!(0)
          end
          
          parts = line.split("=")
          line = "#{parts[0]}=#{parts[1]}=port=#{@config.getProperty(REPL_API_PORT)},host=#{@config.getProperty(REPL_API_HOST)},user=#{@config.getProperty(REPL_API_USER)},password=#{@config.getProperty(REPL_API_PASSWORD)}"
        else
          unless line[0,1] == "#"
            line = "#" + line
          end
        end
        
        line
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
  
  def get_trepctl_cmd
    "#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)}"
  end
  
  def get_dynamic_properties_file()
    "#{get_deployment_basedir()}/tungsten-replicator/conf/dynamic.properties"
  end
  
  def is_replicator?
    true
  end
  
  def is_master?
    @config.getPropertyOr(REPL_SERVICES, {}).each{
      |service_alias,service_properties|
      
      if service_properties[REPL_ROLE] == REPL_ROLE_M
        return true
      end
    }
    
    false
  end
end
