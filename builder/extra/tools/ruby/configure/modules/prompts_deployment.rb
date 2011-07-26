class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts")
      
    ClusterHostPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
end

module ClusterHostPrompt
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
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
    
    super(DEPLOYMENT_TYPE, "Deployment type (#{deployment_types.join(',')})", 
      validator, deployment_types[0])
  end
end

class DeploymentServicePrompt < ConfigurePrompt
  include AdvancedPromptModule
  
  def initialize
    super(DEPLOYMENT_SERVICE, "Deployment Service", 
      PV_ANY, DISTRIBUTED_DEPLOYMENT_NAME)
  end
  
  def enabled?
    super() && [SERVICE_CREATE, SERVICE_DELETE, SERVICE_UPDATE].include?(@config.getPropertyOr(DEPLOYMENT_TYPE, ""))
  end
end

class DeployCurrentPackagePrompt < ConfigurePrompt
  def initialize
    super(DEPLOY_CURRENT_PACKAGE, "Deploy the current Tungsten package", PV_BOOLEAN, "true")
  end
  
  def enabled?
    Configurator.instance.get_deployment().require_package_uri() && 
      (Configurator.instance.get_package_path() != nil)
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

class JavaMemorySize < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_JAVA_MEM_SIZE, "Replicator Java heap memory size in Mb (min 128)",
      PV_JAVA_MEM_SIZE, 512)
  end
end

class HostPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(HOST, "DNS hostname", PV_HOSTNAME)
  end
  
  def get_default_value
    get_member()
  end
  
  def allow_group_default
    false
  end
end

class UserIDPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(USERID, "System User", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(HOME_DIRECTORY, "Installation directory", PV_FILENAME)
  end
  
  def get_default_value
    begin
      unless Configurator.instance.is_localhost?(@config.getProperty(get_member_key(HOST)))
        return ssh_result('pwd', false, @config.getProperty(get_member_key(HOST)), @config.getProperty(get_member_key(USERID)))
      end
    rescue => e
    end
    
    if Configurator.instance.is_full_tungsten_package?()
      Configurator.instance.get_base_path()
    else
      ENV['HOME']
    end
  end
  
  def allow_group_default
    false
  end
end

class BaseDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CURRENT_RELEASE_DIRECTORY, "Directory for the latest release", PV_FILENAME)
  end
  
  def get_default_value
    "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
  end
  
  def allow_group_default
    false
  end
end

class TempDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(TEMP_DIRECTORY, "Temporary Directory",
      PV_FILENAME, "/tmp")
  end
end

class InstallServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_INSTALL, "Install service start scripts", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    @config.getProperty(DEPLOYMENT_TYPE) != "sandbox" && 
    (@config.getProperty(get_member_key(USERID)) == "root" || 
      @config.getProperty(get_member_key(ROOT_PREFIX)) == "true")
  end
end

class StartServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_START, "Start services after configuration", 
      PV_BOOLEAN, "false")
  end
  
  def get_prompt
    if @config.getProperty(DBMS_TYPE) == "mysql"
      super()
    else
      "Restart PostgreSQL server and start services after configuration"
    end
  end
  
  def get_prompt_description_filename()
    if @config.getProperty(DBMS_TYPE) == "mysql"
      super()
    else
      "#{get_interface_text_directory()}/prompt_#{@name}_postgresql"
    end
  end
end

class ReportServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(SVC_REPORT, "Report services after configuration", 
      PV_BOOLEAN, "false")
  end
end

class ShellStartupScriptPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(SHELL_STARTUP_SCRIPT, "Filename for the system user shell startup script", 
      PV_SCRIPTNAME)
  end
  
  def get_default_value
    Configurator.instance.get_startup_script_filename(ENV['SHELL'])
  end
end

class RootCommandPrefixPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(ROOT_PREFIX, "Run root commands using sudo", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    @config.getProperty(USERID) != "root"
  end
end

class ReplicationRMIPort < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(REPL_RMI_PORT, "Replication RMI port", 
      PV_INTEGER, 10000)
  end
end

class THLStorageType < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(REPL_LOG_TYPE, "Replicator event log storage (dbms|disk)",
      PV_LOGTYPE, "disk")
  end
  
  def get_disabled_value
    "disk"
  end
end

class THLStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY))
      @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/thl"
    else
      "/opt/continuent/thl"
    end
  end
end

class RelayLogStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Enter the local-disk directory into which the relay-logs will be stored",
		  PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY))
      @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/relay"
    else
      "/opt/continuent/relay"
    end
  end
end

class THLStorageChecksum < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_THL_DO_CHECKSUM, "Execute checksum operations on THL log files", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConnectionTimeout < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_THL_LOG_CONNECTION_TIMEOUT, "Number of seconds to wait for a connection to the THL log", 
      PV_INTEGER, 600)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageRetention < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_THL_LOG_RETENTION, "How long do you want to keep THL files?", 
      PV_ANY, "7d")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageConsistency < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_CONSISTENCY_POLICY, "Should the replicator stop or warn if a consistency check fails?", 
      PV_ANY, "stop")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class THLStorageFileSize < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  include IsReplicatorPrompt
  
  def initialize
    super(REPL_THL_LOG_FILE_SIZE, "File size in bytes for THL disk logs", 
      PV_INTEGER, 100000000)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
end

class DeploymentHost < ConfigurePrompt
  include AdvancedPromptModule
  
  def initialize
    super(DEPLOYMENT_HOST, "Host alias for the host to be deployed here", PV_ANY)
  end
  
  def enabled?
    super() && Configurator.instance.get_deployment().require_deployment_host() &&
      get_value().to_s() == ""
  end
  
  def enabled_for_config?
    super() && Configurator.instance.get_deployment().require_deployment_host() &&
      get_value().to_s() == ""
  end
  
  def get_default_value
    @config.getPropertyOr(HOSTS, {}).each{
      |host_alias, host_props|
    
      if host_props[HOST] == Configurator.instance.hostname()
        if host_props[HOME_DIRECTORY] == Configurator.instance.get_base_path()
          return host_alias
        end
      end
    }
    
    nil
  end
  
  def get_disabled_value
    if Configurator.instance.get_deployment().require_deployment_host()
      get_value()
    else
      nil
    end
  end
end