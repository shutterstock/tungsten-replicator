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
  
  def get_dynamic_properties_file()
    "#{get_deployment_basedir()}/tungsten-replicator/conf/dynamic.properties"
  end
  
  def is_replicator?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      service_hosts = service_properties[REPL_HOSTS].split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        return true
      end
    }
    
    false
  end
  
  def is_master?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      if service_properties[REPL_MASTERHOST] == @config.getProperty(GLOBAL_HOST)
        return true
      end
    }
    
    false
  end
end