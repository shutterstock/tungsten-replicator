class InstallServicesPrompt < ConfigurePrompt
  include GroupConfigurePromptMember
  
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
  include GroupConfigurePromptMember
  
  def initialize
    super(SVC_START, "Start services after configuration", 
      PV_BOOLEAN, "true")
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

class ShellStartupScriptPrompt < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(SHELL_STARTUP_SCRIPT, "Filename for the system user shell startup script", 
      PV_SCRIPTNAME)
  end
  
  def get_default_value
    Configurator.instance.get_startup_script_filename(ENV['SHELL'])
  end
end

class RootCommandPrefixPrompt < AdvancedPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(ROOT_PREFIX, "Run root commands using sudo", 
      PV_BOOLEAN, "false")
  end
  
  def enabled?
    @config.getProperty(USERID) != "root"
  end
end