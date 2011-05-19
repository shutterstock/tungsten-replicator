module ConfigureDeploymentStepReplicator
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

    apply_config_replicator()
    add_service("tungsten-replicator/bin/replicator")
    set_run_as_user("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator")
  end
  
  def write_replication_service_properties
    transformer = Transformer.new(
		  "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.services.properties",
			"#{get_deployment_basedir()}/tungsten-replicator/conf/services.properties", "#")

		transformer.transform { |line|
		  if line =~ /replicator.rmi_port=/ then
        "replicator.rmi_port=" + @config.getProperty(REPL_RMI_PORT)
  		else
  		  line
  		end
		}
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