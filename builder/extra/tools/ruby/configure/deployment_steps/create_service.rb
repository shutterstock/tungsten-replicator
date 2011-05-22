module DeploymentStepCreateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("create_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def create_replication_dataservice
    info("Write the replication service configuration")
    
    service_config = Properties.new()
    service_config.props = @config.props.merge(@config.getProperty([REPL_SERVICES, TEMP_DEPLOYMENT_SERVICE]))
    
    deploy_replication_dataservice(service_config.getProperty(DEPLOYMENT_SERVICE), service_config)
  end
end