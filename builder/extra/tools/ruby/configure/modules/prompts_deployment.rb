class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts")
    self.add_prompts(
      GlobalHostPrompt.new(),
      GlobalIPAddressPrompt.new(),
      UserIDPrompt.new(),
      HomeDirectoryPrompt.new(),
      TempDirectoryPrompt.new(),
      
      ShellStartupScriptPrompt.new(),
      RootCommandPrefixPrompt.new(),
      InstallServicesPrompt.new(),
      StartServicesPrompt.new(),
        
      THLStorageType.new(),
      THLStorageDirectory.new(),
      THLStorageChecksum.new(),
      THLStorageConnectionTimeout.new(),
      THLStorageRetention.new(),
      THLStorageConsistency.new(),
      THLStorageFileSize.new(),
      MySQLRelayLogDirectory.new(),
      
      #ReplicationMonitorInterval.new(),
      JavaMemorySize.new(),
      RMIPort.new()
    )
  end
  
  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) != "sandbox"
  end
end

class ReplicationMonitorInterval < ConstantValuePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_MONITOR_INTERVAL, "Replication monitor interval", 
      PV_INTEGER, 3000)
  end
end

class JavaMemorySize < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_JAVA_MEM_SIZE, "Replicator Java heap memory size in Mb (min 128)",
      PV_JAVA_MEM_SIZE, 512)
  end
end

class RMIPort < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_RMI_PORT, "Replication RMI port", 
      PV_INTEGER, 10000)
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(HOME_DIRECTORY, "Installation directory", PV_FILENAME)
  end
  
  def get_default_value
    begin
      ssh_result('pwd', false, @config.getProperty(get_member_key(HOST)))
    rescue => e
      if Configurator.instance.is_full_tungsten_package?()
        Configurator.instance.get_base_path()
      else
        ENV['HOME']
      end
    end
  end
  
  def allow_group_default
    false
  end
end

class GlobalHostPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(HOST, "DNS hostname", PV_HOSTNAME)
  end
  
  def get_default_value
    Configurator.instance.hostname()
  end
  
  def allow_group_default
    false
  end
end

class GlobalIPAddressPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(IP_ADDRESS, "IP address", PV_HOSTNAME)
  end
  
  def get_default_value
    hostname = @config.getProperty(get_member_key(HOST))
    
    if hostname.to_s() != ""
      Resolv.getaddress(hostname)
    else
      @default
    end
  end
  
  def allow_group_default
    false
  end
end

class UserIDPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(USERID, "System User", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
end

class TempDirectoryPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(TEMP_DIRECTORY, "Temporary Directory",
      PV_FILENAME, "/tmp")
  end
end

class SandboxCountPrompt < ConfigurePrompt
  def initialize
    super(SANDBOX_COUNT, "How many hosts would you like to simulate in the sandbox?", 
      PV_INTEGER)
  end
  
  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) == "sandbox"
  end
end

class DeploymentTypePrompt < ConfigurePrompt
  def initialize
    deployment_types = []
    Configurator.instance.deployments.each {
      |deployment_method|
      deployment_types << deployment_method.get_name()
    }
    
    validator = PropertyValidator.new(deployment_types.join("|"), 
      "Value must be #{deployment_types.join(',')}")
    
    if Configurator.instance.is_full_tungsten_package?
      default = "regular"
    else
      default = "distributed"
    end
      
    super(DEPLOYMENT_TYPE, "Deployment type (#{deployment_types.join(',')})", 
      validator, default)
  end
end

class DeployCurrentPackagePrompt < ConfigurePrompt
  def initialize
    super(DEPLOY_CURRENT_PACKAGE, "Deploy the current Tungsten package", PV_BOOLEAN, "true")
  end
  
  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) != "regular" &&
      Configurator.instance.is_full_tungsten_package?()
  end
  
  def get_disabled_value
    "false"
  end
end

class DeployPackageURIPrompt < ConfigurePrompt
  def initialize
    super(DEPLOY_PACKAGE_URI, "URL for the Tungsten package to deploy", PV_URI,
      get_default_value())
  end

  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) != "regular" && 
      @config.getProperty(DEPLOY_CURRENT_PACKAGE) == "false"
  end
  
  def get_default_value
    "https://s3.amazonaws.com/releases.continuent.com/#{Configurator.instance.get_release_name()}.tar.gz"
  end
end