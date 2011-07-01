module DeploymentStepUpdateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("update_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def update_replication_dataservice
    info("Update the replication service configuration")
    
    service_key = @config.getProperty(DEPLOYMENT_SERVICE)
    service_config = Properties.new()
    service_config.props = @config.props.merge(
      @config.getProperty([REPL_SERVICES, service_key])
    )
    
    deploy_replication_dataservice(
      @config.getProperty([REPL_SERVICES, service_key, DEPLOYMENT_SERVICE]), 
      service_config)
  end
end