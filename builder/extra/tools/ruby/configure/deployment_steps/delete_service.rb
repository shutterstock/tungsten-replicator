module DeploymentStepDeleteService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("delete_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def delete_replication_dataservice
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{@config.getProperty(DEPLOYMENT_SERVICE)} stop")
    cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{@config.getProperty(DEPLOYMENT_SERVICE)} reset")
    
    cmd_result("rm -rf #{@config.getProperty(REPL_LOG_DIR)}")
    
    if @config.getProperty(REPL_RELAY_LOG_DIR)
      cmd_result("rm -rf #{@config.getProperty(REPL_RELAY_LOG_DIR)}")
    end
    
    cmd_result("rm -rf #{@config.getProperty(REPL_SVC_CONFIG_FILE)}")
  end
end