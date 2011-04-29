module DeploymentStepUpdateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("update_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def update_replication_dataservice
    info("Update the replication service configuration")
    deploy_replication_dataservice(@config.getProperty(DEPLOYMENT_SERVICE), @config)
    
    if @config.getProperty(REPL_SVC_START) == "true"
      info("Restart the replication service")
      cmd_result("echo yes | #{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{@config.getProperty(DEPLOYMENT_SERVICE)} stop")
      cmd_result("#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{@config.getProperty(DEPLOYMENT_SERVICE)} start")
    else
      info("Do not restart the replication service")
    end
  end
end