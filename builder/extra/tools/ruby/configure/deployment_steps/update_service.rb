module DeploymentStepUpdateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("update_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def update_replication_dataservice
    info("Update the replication service configuration")
    
    service_config = Properties.new()
    service_config.props = @config.props.merge(@config.getProperty([REPL_SERVICES, TEMP_DEPLOYMENT_SERVICE]))
    
    deploy_replication_dataservice(service_config.getProperty(DEPLOYMENT_SERVICE), service_config)
  end
end