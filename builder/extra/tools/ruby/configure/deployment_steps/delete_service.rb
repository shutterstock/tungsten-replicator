module DeploymentStepDeleteService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("delete_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def delete_replication_dataservice
    service_config = Properties.new()
    service_config.props = @config.props.merge(@config.getProperty([REPL_SERVICES, TEMP_DEPLOYMENT_SERVICE]))
    
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{service_config.getProperty(DEPLOYMENT_SERVICE)} stop")
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{service_config.getProperty(DEPLOYMENT_SERVICE)} reset")
    
    cmd_result("rm -rf #{service_config.getProperty(REPL_LOG_DIR)}")
    
    if service_config.getProperty(REPL_RELAY_LOG_DIR)
      cmd_result("rm -rf #{service_config.getProperty(REPL_RELAY_LOG_DIR)}")
    end
    
    cmd_result("rm -rf #{service_config.getProperty(REPL_SVC_CONFIG_FILE)}")
  end
end