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
      Dataservers.new(),
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
      ReplicationServiceChecks.new()
    ]
  end
end