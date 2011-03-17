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
  
  def get_replication_role
    if is_master?()
      "master"
    else
      "slave"
    end
  end
  
  def get_replication_service_type
    if @config.getProperty(REPL_SVC_SERVICE_TYPE)
      @config.getProperty(REPL_SVC_SERVICE_TYPE)
    elsif @config.getProperty(DSNAME) == @config.getProperty(GLOBAL_DSNAME)
      'local'
    else
      'remote'
    end
  end
  
  def is_replicator?
    (@config.getProperty(REPL_HOSTS).split(",").include?(@config.getProperty(GLOBAL_HOST)))
  end
  
  def is_master?
    (@config.getProperty(REPL_MASTERHOST) == @config.getProperty(GLOBAL_HOST))
  end
end