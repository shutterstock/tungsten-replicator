class ConfigurePackageCluster < ConfigurePackage
  def get_prompts
    [
      DBMSTypePrompt.new(),
      DeploymentTypePrompt.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      ConfigurePrompt.new(CLUSTERNAME, "Cluster Name", 
        PV_IDENTIFIER, "default"),
      ClusterHosts.new(),
      DeploymentHost.new(),
      
      DataServers.new()
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
      THLStorageCheck.new(),
      DataserversChecks.new(),
      BackupMethodAvailableCheck.new(),
    ]
  end
end