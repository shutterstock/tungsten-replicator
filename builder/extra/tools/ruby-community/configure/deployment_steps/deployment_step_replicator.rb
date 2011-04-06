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
    @config.getProperty(REPL_SERVICES).split(",").each{
      |service_name|
      service_properties = @config.getProperty(Configurator::SERVICE_CONFIG_PREFIX + service_name)
      
      unless service_properties
        raise "Unable to find service configuration for '#{service_name}'"
      end
      
      unless service_properties[REPL_HOSTS]
        raise "Missing replication hosts definition for '#{service_name}' configuration"
      end
      
      service_hosts = service_properties[REPL_HOSTS].split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        return true
      end
    }
  end
  
  def is_master?
    @config.getProperty(REPL_SERVICES).split(",").each{
      |service_name|
      service_properties = @config.getProperty(Configurator::SERVICE_CONFIG_PREFIX + service_name)
      
      unless service_properties
        raise "Unable to find service configuration for '#{service_name}'"
      end
      
      unless service_properties[REPL_MASTER_HOST]
        raise "Missing replication master definition for '#{service_name}' configuration"
      end
      
      return (service_properties[REPL_MASTER_HOST].split(",") == @config.getProperty(GLOBAL_HOST))
    }
  end
end