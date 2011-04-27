module DeploymentStepCreateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("create_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def create_replication_dataservice
    info("Write the replication service configuration")
    deploy_replication_dataservice(@config.getProperty(DEPLOYMENT_SERVICE), @config)
    
    if @config.getProperty(REPL_SVC_START) == "true"
      info("Start the replication service")
      cmd_result("#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -service #{@config.getProperty(DEPLOYMENT_SERVICE)} start")
    else
      info("Do not start the replication service")
    end
  end
end