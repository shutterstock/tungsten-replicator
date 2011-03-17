class InstallServicesPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_SVC_INSTALL, "Install service start scripts", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    @config.getProperty(GLOBAL_DEPLOYMENT_TYPE) != "sandbox" && 
    (@config.getProperty(GLOBAL_USERID) == "root" || 
      @config.getProperty(GLOBAL_ROOT_PREFIX) == "true")
  end
end

class StartServicesPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_SVC_START, "Start services after configuration", PV_BOOLEAN, "true")
  end
  
  def get_prompt
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      @prompt
    else
      "Restart PostgreSQL server and start services after configuration"
    end
  end
  
  def get_prompt_description_filename(prompt_name)
    if @config.getProperty(GLOBAL_DBMS_TYPE) == "mysql"
      super(prompt_name)
    else
      "#{get_interface_text_directory()}/prompt_#{prompt_name}_postgresql"
    end
  end
end

class ShellStartupScriptPrompt < ConfigurePrompt
  def initialize
    super(SHELL_STARTUP_SCRIPT, "Filename for the system user shell startup script", PV_SCRIPTNAME)
  end
  
  def get_default_value
    Configurator.instance.get_startup_script_filename(ENV['SHELL'])
  end
end

class RootCommandPrefixPrompt < ConfigurePrompt
  def initialize
    super(GLOBAL_ROOT_PREFIX, "Run root commands using sudo", 
      PV_BOOLEAN, "true")
  end
  
  def enabled?
    @config.getProperty(GLOBAL_USERID) != "root"
  end
end