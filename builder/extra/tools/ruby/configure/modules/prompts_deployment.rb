class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts")
    
    @allowed_group_members = 1  
      
    self.add_prompts(
      GlobalHostPrompt.new(),
      GlobalIPAddressPrompt.new(),
      UserIDPrompt.new(),
      HomeDirectoryPrompt.new(),
      BaseDirectoryPrompt.new(),
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
      RMIPort.new(),
      
      BackupMethod.new(),
      BackupStorageDirectory.new(),
      BackupConfigurePrompt.new(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory",
        PV_FILENAME, "/tmp"),
      BackupConfigurePrompt.new(REPL_BACKUP_RETENTION, "Number of backups to retain", 
        PV_INTEGER, 3),
      BackupScriptPathConfigurePrompt.new(),
      BackupScriptCommandPrefixConfigurePrompt.new(),
      BackupScriptOnlineConfigurePrompt.new()
    )
  end
  
  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) != "sandbox"
  end
  
  def default_member_alias(member_key)
    DIRECT_DEPLOYMENT_HOST_ALIAS
  end
end

class DBMSTypePrompt < ConfigurePrompt
  def initialize
    super(DBMS_TYPE, "Database type (mysql, or postgresql)", PV_DBMSTYPE)
  end
  
  def get_default_value
    case Configurator.instance.whoami()
    when "postgres"
      return "postgresql"
    when "enterprisedb"
      return "postgresql"
    else
      return "mysql"
    end
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
    if @config.getProperty(DEPLOYMENT_TYPE) == DIRECT_DEPLOYMENT_NAME
      Configurator.instance.get_base_path()
    else
      begin
        ssh_result('pwd', false, @config.getProperty(get_member_key(HOST)), @config.getProperty(get_member_key(USERID)))
      rescue => e
        if Configurator.instance.is_full_tungsten_package?()
          Configurator.instance.get_base_path()
        else
          ENV['HOME']
        end
      end
    end
  end
  
  def allow_group_default
    false
  end
end

class BaseDirectoryPrompt < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(BASEDIR, "Directory for the latest release", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(DEPLOYMENT_TYPE) == DIRECT_DEPLOYMENT_NAME
      @config.getProperty(get_member_key(HOME_DIRECTORY))
    else
      "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
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
    if @config.getProperty(DEPLOYMENT_TYPE) == DIRECT_DEPLOYMENT_NAME
      Configurator.instance.hostname()
    else
      get_member()
    end
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
    Configurator.instance.get_deployments().each {
      |deployment|
      if deployment.include_deployment_for_package?(Configurator.instance.package)
        deployment_types << deployment.get_name()
      end
    }
    
    validator = PropertyValidator.new(deployment_types.join("|"), 
      "Value must be #{deployment_types.join(',')}")
    
    if Configurator.instance.is_full_tungsten_package?
      default = DIRECT_DEPLOYMENT_NAME
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
    Configurator.instance.get_deployment().require_package_uri() && 
      Configurator.instance.is_full_tungsten_package?()
  end
end

class DeployPackageURIPrompt < ConfigurePrompt
  def initialize
    super(DEPLOY_PACKAGE_URI, "URL for the Tungsten package to deploy", PV_URI,
      get_default_value())
  end

  def enabled?
    Configurator.instance.get_deployment().require_package_uri() && 
      @config.getProperty(DEPLOY_CURRENT_PACKAGE) != "true"
  end
  
  def get_default_value
    "https://s3.amazonaws.com/releases.continuent.com/#{Configurator.instance.get_release_name()}.tar.gz"
  end
end

class DeploymentHost < ConfigurePrompt
  def initialize
    super(DEPLOYMENT_HOST, "Host alias for the host to be deployed here", PV_ANY)
  end
  
  def enabled?
    Configurator.instance.get_deployment().require_deployment_host() &&
      get_value() == ""
  end
  
  def get_default_value
    if @config.getProperty(DEPLOYMENT_TYPE) == DIRECT_DEPLOYMENT_NAME
      return DIRECT_DEPLOYMENT_HOST_ALIAS
    else
      @config.getPropertyOr(HOSTS, {}).each{
        |host_alias, host_props|
      
        if host_props[HOST] == Configurator.instance.hostname()
          if host_props[HOME_DIRECTORY] == Configurator.instance.get_base_path()
            return host_alias
          end
        end
      }
    end
    
    ""
  end
  
  def get_disabled_value
    if Configurator.instance.get_deployment().require_deployment_host()
      get_value()
    else
      nil
    end
  end
end