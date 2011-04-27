class ClusterConfigureModule < ConfigureModule
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