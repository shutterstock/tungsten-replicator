class ConfigureValidationCheck
  include ValidationCheckInterface
  
  def is_connector?
    (@config.getPropertyOr(CONN_HOSTS, "").split(",").include?(@config.getProperty(GLOBAL_HOST)))
  end
  
  def is_replicator?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      service_hosts = service_properties[REPL_HOSTS].split(",")
      if service_hosts.include?(@config.getProperty(GLOBAL_HOST))
        return true
      end
    }
    
    false
  end
  
  def is_master?
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      if service_properties[REPL_MASTERHOST] == @config.getProperty(GLOBAL_HOST)
        return true
      end
    }
    
    false
  end
end

module LocalValidationCheck
end