module DeploymentStepCreateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("create_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def create_replication_dataservice
    info("Write the replication service configuration")
    
    deploy_replication_dataservice()
    
    service_key = @config.getProperty(DEPLOYMENT_SERVICE)
    stored_config = Properties.new
    stored_config.load(Configurator.instance.get_config_filename)
    stored_config.setProperty(
      [REPL_SERVICES, service_key],
      @config.getProperty([REPL_SERVICES, service_key])
    )
    stored_config.store(Configurator.instance.get_config_filename)
  end
end