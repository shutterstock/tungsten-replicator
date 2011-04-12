class ClusterConfigureModule < ConfigureModule
  def initialize
    super()
    @weight = -5
  end
  
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      ShellStartupScriptPrompt.new(),
      RootCommandPrefixPrompt.new(),
      InstallServicesPrompt.new(),
      StartServicesPrompt.new()
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
      ClusterSSHLoginCheck.new(),
      InstallServicesCheck.new(),
    ])
  end
  
  def include_module_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    else
      false
    end
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