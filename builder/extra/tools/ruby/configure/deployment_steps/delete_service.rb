module DeploymentStepDeleteService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("delete_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def delete_replication_dataservice
    service_key = @config.getProperty(DEPLOYMENT_SERVICE)
    
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)} -service #{@config.getProperty([REPL_SERVICES, service_key, DEPLOYMENT_SERVICE])} stop")
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)} -service #{@config.getProperty([REPL_SERVICES, service_key, DEPLOYMENT_SERVICE])} reset")
    
    cmd_result("rm -rf #{@config.getProperty([REPL_SERVICES, service_key, REPL_LOG_DIR])}")
    
    if @config.getProperty([REPL_SERVICES, service_key, REPL_RELAY_LOG_DIR])
      cmd_result("rm -rf #{@config.getProperty([REPL_SERVICES, service_key, REPL_RELAY_LOG_DIR])}")
    end
    
    cmd_result("rm -rf #{@config.getProperty([REPL_SERVICES, service_key, REPL_SVC_CONFIG_FILE])}")
    
    stored_config = Properties.new
    stored_config.load(get_deployment_config_file())
    stored_config.setProperty(
      [REPL_SERVICES, service_key],
      nil
    )
    stored_config.store(get_deployment_config_file())
  end
end