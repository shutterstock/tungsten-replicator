class ClusterHosts < GroupConfigurePrompt
  def initialize
    super(HOSTS, "Enter host information for @value", 
      "host", "hosts", "HOST")
      
    ClusterHostPrompt.subclasses().each{
      |klass|
      self.add_prompt(klass.new())
    }
  end
  
  def update_deprecated_keys()
    each_member{
      |member|
      
      @config.setProperty([HOSTS, member, 'shell_startup_script'], nil)
    }
    
    super()
  end
end

# Prompts that include this module will be collected for each host 
# across interactive mode, the configure script and the
# tungsten-installer script
module ClusterHostPrompt
  def self.included(subclass)
    @subclasses ||= []
    @subclasses << subclass
  end

  def self.subclasses
    @subclasses || []
  end
  
  def get_command_line_argument()
    super.gsub("repl-", "")
  end
end

class DeploymentTypePrompt < ConfigurePrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  
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
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_SERVICE, "Deployment Service", 
      PV_ANY)
  end
end

class DeployCurrentPackagePrompt < ConfigurePrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOY_CURRENT_PACKAGE, "Deploy the current Tungsten package", PV_BOOLEAN, "true")
  end
  
  def enabled?
    Configurator.instance.get_deployment().require_package_uri() && 
      (Configurator.instance.get_package_path() != nil)
  end
end

class DeployPackageURIPrompt < ConfigurePrompt
  include NoTemplateValuePrompt

  def initialize
    super(DEPLOY_PACKAGE_URI, "URL for the Tungsten package to deploy", PV_URI)
  end

  def enabled?
    Configurator.instance.get_deployment().require_package_uri() && 
      @config.getProperty(DEPLOY_CURRENT_PACKAGE) != "true"
  end
  
  def get_default_value
    if enabled?
      "https://s3.amazonaws.com/releases.continuent.com/#{Configurator.instance.get_release_name()}.tar.gz"
    else
      nil
    end
  end
end

class ClusterNamePrompt < ConfigurePrompt
  def initialize
    super(CLUSTERNAME, "Cluster Name", PV_IDENTIFIER, "default")
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

  def enabled_for_command_line?()
    false
  end
end

class UserIDPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include NotTungstenUpdatePrompt
  
  def initialize
    super(USERID, "System User", 
      PV_IDENTIFIER, Configurator.instance.whoami())
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('userid'))
    super()
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include NotTungstenUpdatePrompt
  
  def initialize
    super(HOME_DIRECTORY, "Installation directory", PV_FILENAME)
  end
  
  def get_default_value
    begin
      if Configurator.instance.display_help? && !Configurator.instance.display_preview?
        raise ""
      end
      
      unless Configurator.instance.is_localhost?(@config.getProperty(get_member_key(HOST)))
        Timeout.timeout(2) {
          return ssh_result('pwd', @config.getProperty(get_member_key(HOST)), @config.getProperty(get_member_key(USERID)))
        }
      end
    rescue Timeout::Error
    rescue => e
    end
    
    if Configurator.instance.is_full_tungsten_package?()
      return Configurator.instance.get_base_path()
    else
      return ENV['HOME']
    end
  end
end

class BaseDirectoryPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
  def initialize
    super(CURRENT_RELEASE_DIRECTORY, "Directory for the latest release", PV_FILENAME)
  end
  
  def get_default_value
    if @config.getProperty(get_member_key(HOME_DIRECTORY)) == Configurator.instance.get_base_path()
      @config.getProperty(get_member_key(HOME_DIRECTORY))
    else
      "#{@config.getProperty(get_member_key(HOME_DIRECTORY))}/#{Configurator::CURRENT_RELEASE_DIRECTORY}"
    end
  end
  
  def allow_group_default
    false
  end
  
  def enabled_for_command_line?()
    false
  end
end

class TempDirectoryPrompt < ConfigurePrompt
  include AdvancedPromptModule
  include ClusterHostPrompt
  
  def initialize
    super(TEMP_DIRECTORY, "Temporary Directory",
      PV_FILENAME, "/tmp")
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
    super() && @config.getProperty(USERID) != "root"
  end
  
  def get_template_value(transform_values_method)
    if get_value() == "true"
      "sudo"
    else
      ""
    end
  end
end

class ReplicationRMIPort < ConfigurePrompt
  include ClusterHostPrompt
  
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
  include NotTungstenUpdatePrompt
  
  def initialize
    super(REPL_LOG_DIR, "Replicator log directory", PV_FILENAME)
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_LOG_TYPE)) == "disk"
  end
  
  def get_default_value
    @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/thl"
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_log_dir'))
    super()
  end
end

class RelayLogStorageDirectory < ConfigurePrompt
  include ClusterHostPrompt
  include NotTungstenUpdatePrompt
  
  def initialize
    super(REPL_RELAY_LOG_DIR, "Directory for logs transferred from the master",
		  PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/relay"
  end
  
  def update_deprecated_keys()
    replace_deprecated_key(get_member_key('repl_relay_log_dir'))
    super()
  end
end

class BackupStorageDirectory < BackupConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(REPL_BACKUP_STORAGE_DIR, "Permanent backup storage directory", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(HOME_DIRECTORY)) + "/backups"
  end
end

class DeploymentHost < ConfigurePrompt
  include AdvancedPromptModule
  include NoTemplateValuePrompt
  
  def initialize
    super(DEPLOYMENT_HOST, "Host alias for the host to be deployed here", PV_ANY)
  end
  
  def enabled?
    super() && Configurator.instance.get_deployment().require_deployment_host()
  end
  
  def get_command_line_argument
    nil
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

class InstallServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule
  
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
    super(SVC_START, "Start the replicator after configuration", 
      PV_BOOLEAN)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(SVC_REPORT))
  end
  
  def get_command_line_argument_value
    "true"
  end
end

class ReportServicesPrompt < ConfigurePrompt
  include ClusterHostPrompt
  
  def initialize
    super(SVC_REPORT, "Start the replicator and report out the services list after configuration", 
      PV_BOOLEAN, "false")
  end
  
  def get_command_line_argument_value
    "true"
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

class ReplicationAPI < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API, "Enable the replication API", PV_BOOLEAN, "false")
  end
end

class ReplicationAPIHost < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_HOST, "Hostname that the replication API should listen on", PV_HOSTNAME, "localhost")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPort < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_PORT, "Port that the replication API should bind to", PV_INTEGER, "19999")
  end

  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIUser < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_USER, "HTTP basic auth username for the replication API", PV_ANY, "tungsten")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class ReplicationAPIPassword < ConfigurePrompt
  include ClusterHostPrompt
  include AdvancedPromptModule

  def initialize
    super(REPL_API_PASSWORD, "HTTP basic auth password for the replication API", PV_ANY, "secret")
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end

  def enabled_for_config?
    super() && @config.getProperty(get_member_key(REPL_API)) == "true"
  end
end

class HostEnableReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include HiddenValueModule
  
  def initialize
    super(HOST_ENABLE_REPLICATOR, "Is the replicator enabled on this host", PV_BOOLEAN, "true")
  end
end

class HostServicePathReplicator < ConfigurePrompt
  include ClusterHostPrompt
  include ConstantValueModule
  
  def initialize
    super(SVC_PATH_REPLICATOR, "Path to the replicator service command", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(CURRENT_RELEASE_DIRECTORY)) + "/tungsten-replicator/bin/replicator"
  end
end