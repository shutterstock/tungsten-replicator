class InstallServicesCheck < ConfigureValidationCheck
  def set_vars
    @title = "Install services check"
  end
  
  def validate
    if File.exist?("/etc/redhat-release")
      info("OS supports service installation")
    elsif File.exist?("/etc/debian_version")
      info("OS supports service installation")
    else
      error("OS is unable to support service installation")
    end
  end
  
  def enabled?
    (@config.getProperty(GLOBAL_SVC_INSTALL) == "true")
  end
end

class ClusterSSHLoginCheck < ConfigureValidationCheck
  def set_vars
    @title = "SSH login check"
  end
  
  def validate
    @config.getProperty(REPL_HOSTS).split(",").each{
      |repl_host|
      remote_user = ssh_result("echo $USER", false, repl_host, @config.getProperty(GLOBAL_USERID))
      if remote_user != @config.getProperty(GLOBAL_USERID)
        error("SSH login failed from #{@config.getProperty(GLOBAL_HOST)} to #{repl_host}")
      else
        info("SSH login successful from #{@config.getProperty(GLOBAL_HOST)} to #{repl_host}")
      end
    }
  end
end