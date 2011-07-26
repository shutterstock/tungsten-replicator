class ConfigurePackageCluster < ConfigurePackage
  def get_prompts
    [
      DeploymentTypePrompt.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      ConfigurePrompt.new(CLUSTERNAME, "Cluster Name", 
        PV_IDENTIFIER, "default"),
      DeploymentHost.new(),
      ClusterHosts.new(),
      Dataservers.new(),
      ReplicationServices.new()
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