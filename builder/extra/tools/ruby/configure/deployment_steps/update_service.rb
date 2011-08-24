module DeploymentStepUpdateService
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("update_replication_dataservice")
    ]
  end
  module_function :get_deployment_methods
  
  def update_replication_dataservice
    info("Update the replication service configuration")
    
    deploy_replication_dataservice()
    
    service_key = @config.getProperty(DEPLOYMENT_SERVICE)
    stored_config = Properties.new
    stored_config.load(get_deployment_config_file())
    stored_config.setProperty(
      [REPL_SERVICES, service_key],
      @config.getProperty([REPL_SERVICES, service_key])
    )
    
    applier = @config.getProperty([REPL_SERVICES, service_key, REPL_DATASOURCE])
    extractor = @config.getProperty([REPL_SERVICES, service_key, REPL_MASTER_DATASOURCE])
    
    if applier != ""
      stored_config.setProperty(
        [DATASOURCES, applier],
        @config.getProperty([DATASOURCES, applier])
      )
    end
    
    if extractor != ""
      stored_config.setProperty(
        [DATASOURCES, extractor],
        @config.getProperty([DATASOURCES, extractor])
      )
    end
    
    stored_config.store(get_deployment_config_file())
  end
end