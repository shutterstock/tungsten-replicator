class ManagerConfigureModule < ConfigureModule
  def initialize
    super()
    @weight = 10
  end
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      ConstantValuePrompt.new(GLOBAL_GC_MEMBERSHIP, "Group communication protocol", 
        PV_ANY, "ping"),
      WitnessHostsPrompt.new(),
      ManagerModePrompt.new(),
      AdvancedInterfaceMessage.new('mgr_ping_timeout', "Specify rules failure detection parameters"),
      AdvancedPrompt.new(MGR_DB_PING_TIMEOUT, "Timeout in seconds when executing a query to determine DB liveness", PV_INTEGER, 15),
      AdvancedPrompt.new(MGR_HOST_PING_TIMEOUT, "Timeout in seconds when executing a ping to determine HOST liveness", PV_INTEGER, 5),
      AdvancedInterfaceMessage.new("mgr_liveness", "Specify rules failure detection parameters"),
      AdvancedPrompt.new(MGR_IDLE_ROUTER_TIMEOUT, "Time delay, in seconds, before disconnecting an idle router", PV_INTEGER, 3600),
      AdvancedPrompt.new(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS, "Rules engine liveness sample period, in seconds", PV_INTEGER, 2),
      AdvancedPrompt.new(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD, "Number of rules engine liveness sample periods before interrupt", PV_INTEGER, 30),
      AdvancedInterfaceMessage.new("mgr_notifications", "Specify router notification parameters"),
      AdvancedPrompt.new(MGR_NOTIFICATIONS_SEND, "Enable notifications?", PV_BOOLEAN, "true"),
      ManagerNotificationsTimeoutPrompt.new(),
      ConstantValuePrompt.new(MON_DB_QUERY_TIMEOUT, "Number of seconds before a DB check query will time out", PV_INTEGER, 5),
      ConstantValuePrompt.new(MON_DB_CHECK_FREQUENCY, "Number of milliseconds between DB check queries", PV_INTEGER, 3000),
      ConstantValuePrompt.new(MON_REPLICATOR_CHECK_FREQUENCY, "Number of milliseconds between replicator checks", PV_INTEGER, 3000),
      ConstantValuePrompt.new(MGR_POLICY_FAIL_THRESHOLD, "", PV_INTEGER, 4),
      ConstantValuePrompt.new(MGR_POLICY_FENCE_SLAVE_REPLICATOR, "", PV_BOOLEAN, "true"),
      ConstantValuePrompt.new(MGR_POLICY_FENCE_MASTER_REPLICATOR, "", PV_BOOLEAN, "false"),
      ConstantValuePrompt.new(OPTION_MONITOR_INTERNAL, "Use internal monitor", PV_BOOLEAN, "true")
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
      WitnessHostAvailableCheck.new(),
    ])
  end
end

class ManagerNotificationsTimeoutPrompt < AdvancedPrompt
  def initialize
    super(MGR_NOTIFICATIONS_TIMEOUT, "Number of milliseconds before a router notification will time out", PV_INTEGER, 30000)
  end
  
  def enabled?
    super() && (@config.getProperty(MGR_NOTIFICATIONS_SEND) == "true")
  end
end

class ManagerModePrompt < ConfigurePrompt
  def initialize
    super(POLICY_MGR_MODE, "Default failover mode (manual|automatic)", PV_POLICY_MGR_MODE, "automatic")
  end
  
  def enabled?
    Configurator.instance.tungsten_version() == Configurator::TUNGSTEN_ENTERPRISE
  end
  
  def get_disabled_value
    "manual"
  end
end

class WitnessHostsPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_WITNESSES, "Enter a comma-delimited list of witness hosts", 
      PV_HOSTNAME)
  end
  
  def enabled?
    (@config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "sandbox" && Configurator.instance.is_enterprise?())
  end
end

require 'ping'
class WitnessHostAvailableCheck < ConfigureValidationCheck
  def set_vars
    @title = "Witness hosts check"
  end
  
  def validate
    @config.getProperty(GLOBAL_WITNESSES).split(",").each{
      |host|
      if Ping.pingecho(host, 5)
        info("Able to contact witness host: #{host}")
      else
        error("Unable to contact witness host: #{host}")
      end
    }
  end
end