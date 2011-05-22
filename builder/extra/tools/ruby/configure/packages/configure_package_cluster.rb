class ConfigurePackageCluster < ConfigurePackage
  def get_prompts
    repl_services = ReplicationServices.new()
    repl_services.extend(AdvancedPromptModule)
    
    [
      DeploymentTypePrompt.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      ConfigurePrompt.new(CLUSTERNAME, "Cluster Name", 
        PV_IDENTIFIER, "default"),
      ClusterHosts.new(),
      DeploymentHost.new(),
      DataServers.new(),
      repl_services
    ]
  end
  
  def get_validation_checks
    [
      ClusterSSHLoginCheck.new(),
      InstallServicesCheck.new(),
      SSHLoginCheck.new(),
      WriteableTempDirectoryCheck.new(),
      WriteableHomeDirectoryCheck.new(),
      DeploymentPackageCheck.new(),
      RubyVersionCheck.new(),
      JavaVersionCheck.new(),
      OSCheck.new(),
      SudoCheck.new(),
      HostnameCheck.new(),
      PackageDownloadCheck.new(),
      TransferredLogStorageCheck.new(),
      DataserversChecks.new(),
      BackupMethodAvailableCheck.new(),
      ReplicationServiceChecks.new()
    ]
  end
  
  def self.services_list(config)
    config.getPropertyOr(REPL_SERVICES, "").split(",")
  end
  
  def self.each_service(config, &f)
    self.services_list(config).each{
      |service_name|
      parent_name = Configurator::SERVICE_CONFIG_PREFIX + service_name
      
      f.call(parent_name, service_name, config.getProperty(parent_name))
    }
  end
end