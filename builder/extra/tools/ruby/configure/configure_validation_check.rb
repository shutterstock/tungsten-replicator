class ConfigureValidationCheck
  include ValidationCheckInterface
  
  def is_connector?
    false
  end
  
  def is_replicator?
    @config.getPropertyOr(REPL_SERVICES, {}).size() > 0
  end
  
  def is_master?
    @config.getPropertyOr(REPL_SERVICES, {}).each{
      |service_alias,service_properties|
      
      if service_properties[REPL_ROLE] == REPL_ROLE_M
        return true
      end
    }
    
    false
  end
end

module LocalValidationCheck
end