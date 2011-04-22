class DeploymentConfigureModule < ConfigureModule
  def initialize
    super()
    @weight = -10
  end
  
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      ConfigurePrompt.new(GLOBAL_DBMS_TYPE, "Database type (mysql, or postgresql)", PV_DBMSTYPE),
      DeploymentTypePrompt.new(),
      DeployCurrentPackagePrompt.new(),
      DeployPackageURIPrompt.new(),
      ConfigurePrompt.new(GLOBAL_CLUSTERNAME, "Cluster Name", 
        PV_IDENTIFIER, "default"),
      ClusterHosts.new()
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
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
    ])
  end
  
  def include_module_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    else
      false
    end
  end
end