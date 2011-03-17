class ClusterHostsPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_HOSTS, "Enter a comma-delimited list of cluster member hosts", 
      PV_HOSTNAME)
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "sandbox"
  end
end

class HomeDirectoryPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_HOME_DIRECTORY, "Installation directory", PV_FILENAME)
  end
  
  def get_default_value
    begin
      hosts = @config.getProperty(GLOBAL_HOSTS).split(",")
      hostname = hosts[0]
      ssh_result('pwd', false, hostname)
    rescue => e
      ENV['HOME']
    end
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "direct"
  end
end

class GlobalHostPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_HOST, "Name of this host", PV_HOSTNAME)
  end
  
  def get_default_value
    Configurator.instance.hostname()
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) == "direct"
  end
end

class GlobalIPAddressPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_IP_ADDRESS, "IP address for this host", PV_HOSTNAME)
  end
  
  def get_default_value
    Resolv.getaddress(@config.getProperty(GLOBAL_HOST))
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) == "direct"
  end
end

class SandboxCountPrompt < ConfigurePrompt
  def initialize
    super(SANDBOX_COUNT, "How many hosts would you like to simulate in the sandbox?", 
      PV_INTEGER)
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) == "sandbox"
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
      
    super(GLOBAL_DEPLOYMENT_TYPE, "Deployment type (#{deployment_types.join(',')})", 
      validator, "regular")
  end
end

class DeployCurrentPackagePrompt < ConfigurePrompt
  def initialize
    super(DEPLOY_CURRENT_PACKAGE, "Deploy the current Tungsten package", PV_BOOLEAN, "true")
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "direct" &&
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
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "direct" && 
      @config.getProperty(DEPLOY_CURRENT_PACKAGE) == "false"
  end
  
  def get_default_value
    "https://s3.amazonaws.com/releases.continuent.com/#{Configurator.instance.get_release_name()}.tar.gz"
  end
end