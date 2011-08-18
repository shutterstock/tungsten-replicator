class SSHLoginCheck < ConfigureValidationCheck
  include LocalValidationCheck
  def set_vars
    @title = "SSH login"
    @description = "Ensure that the configuration host can login to each member of the cluster via SSH"
    @properties << USERID
    @fatal_on_error = true
    @weight = -5
  end
  
  def validate
    # whoami will output the current user and we can confirm that the login succeeded
    begin
      login_result = ssh_result("whoami", @config.getProperty(HOST), @config.getProperty(USERID))
    rescue RemoteError
      login_result = ""
    end
    
    if login_result != @config.getProperty(USERID)
      error "Unable to SSH to #{@config.getProperty(HOST)} as #{@config.getProperty(USERID)}."
      help "Ensure that the host is running and that you can login via SSH using key authentication"
    else
      debug "SSH login successful"
    end
  end
  
  def enabled?
    (@config.getProperty(HOST) != Configurator.instance.hostname())
  end
end

class WriteableTempDirectoryCheck < ConfigureValidationCheck
  include LocalValidationCheck
  def set_vars
    @title = "Writeable temp directory"
    @properties << TEMP_DIRECTORY
  end
  
  def validate
    validation_temp_directory = "#{@config.getProperty(TEMP_DIRECTORY)}/#{Configurator.instance.get_unique_basename()}/"
    debug "Checking #{validation_temp_directory}"
    
    begin
      ssh_result("mkdir -p #{validation_temp_directory}", @config.getProperty(HOST), @config.getProperty(USERID))
    rescue RemoteCommandError => rce
      error("Unable to create the temporary directory '#{validation_temp_directory}':#{rce.result}")
      
      # Do not process the other parts of this check
      return
    end
    
    # The -D flag will tell us if it is a directory
    is_directory = ssh_result("if [ -d #{validation_temp_directory} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    unless is_directory == "0"
      error "#{validation_temp_directory} is not a directory"
    else
      debug "#{validation_temp_directory} is a directory"
    end
    
    # The -w flag will tell us if it is writeable
    writeable = ssh_result("if [ -w #{validation_temp_directory} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    
    unless writeable == "0"
      error "#{validation_temp_directory} is not writeable"
    else
      debug "#{validation_temp_directory} is writeable"
    end
  end
  
  def enabled?
    (@config.getProperty(HOST) != Configurator.instance.hostname())
  end
end

class WriteableHomeDirectoryCheck < ConfigureValidationCheck
  include LocalValidationCheck
  def set_vars
    @title = "Writeable home directory"
    @properties << HOME_DIRECTORY
  end
  
  def validate
    debug "Checking #{@config.getProperty(HOME_DIRECTORY)} can be created"
    
    if @config.getProperty(HOME_DIRECTORY) =~ /^#{Configurator.instance.get_base_path()}[\/]?.*/ &&
        !(@config.getProperty(HOME_DIRECTORY) =~ /^#{Configurator.instance.get_base_path()}[\/]?$/)
      error("Unable to create the home directory as a sub-directory of the current package")
    end
    
    if @config.getProperty(HOME_DIRECTORY) == @config.getProperty(CURRENT_RELEASE_DIRECTORY)
      dir = File.dirname(@config.getProperty(HOME_DIRECTORY))
    else
      dir = @config.getProperty(HOME_DIRECTORY)
    end
    
    ssh_result("mkdir -p #{dir}", @config.getProperty(HOST), @config.getProperty(USERID))
    
    # The -d flag will tell us if it is a directory
    is_directory = ssh_result("if [ -d #{dir} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    
    unless is_directory == "0"
      error "#{dir} is not a directory"
    else
      debug "#{dir} is a directory"
    end
    
    # The -w flag will tell us if it is writeable
    writeable = ssh_result("if [ -w #{dir} ]; then echo 0; else echo 1; fi", @config.getProperty(HOST), @config.getProperty(USERID))
    
    unless writeable == "0"
      error "#{dir} is not writeable"
    else
      debug "#{dir} is writeable"
    end
  end
  
  def enabled?
    @config.getProperty(HOME_DIRECTORY) != nil
  end
end

class DeploymentPackageCheck < ConfigureValidationCheck
  include LocalValidationCheck
  def set_vars
    @title = "Deployment package"
  end
  
  def validate
    uri = URI::parse(@config.getProperty(GLOBAL_DEPLOY_PACKAGE_URI))
    if uri.scheme == "file" && (uri.host == nil || uri.host == "localhost")
      debug("Send deployment package to #{@config.getProperty(HOST)}")
      cmd_result("rsync -aze ssh --delete #{uri.path} #{@config.getProperty(USERID)}@#{@config.getProperty(HOST)}:#{@config.getProperty(TEMP_DIRECTORY)}")
    end
  end
  
  def enabled?
    @config.getProperty(DEPLOY_PACKAGE_URI) && !(Configurator.instance.is_localhost?(@config.getProperty(HOST)))
  end
end

class RubyVersionCheck < ConfigureValidationCheck
  include LocalValidationCheck
  def set_vars
    @title = "Ruby version"
  end
  
  def validate
    ruby_version = ssh_result("ruby -v | cut -f 2 -d ' '", @config.getProperty(HOST), @config.getProperty(USERID))
    
    if ruby_version =~ /^1\.8\.[5-9]/
      debug "Ruby version (#{ruby_version}) OK"
    elsif ruby_version =~ /^1\.8/
      error "Ruby version must be at least 1.8.5"
    elsif ruby_version =~ /^1\.9/
      warning "Ruby version may not work; try Ruby 1.8.5-1.8.7"
    else
      error "Unrecognizable Ruby version: #{ruby_version}"
    end
  end
end

class OSCheck < ConfigureValidationCheck
  def set_vars
    @title = "Operating system"
  end
  
  def validate
    # Check operating system.
    debug "Checking operating system type"
    uname = cmd_result("uname -a")
    uname_s = cmd_result("uname -s")
    os = case
    when uname_s == "Linux" then OS_LINUX
    when uname_s == "Darwin" then OS_MACOSX
    when uname_s == "SunOS" then OS_SOLARIS
    else OS_UNKNOWN
    end
    if os == OS_UNKNOWN
      raise "Could not determine OS!  Tungsten currently supports Linux, Solaris or OS X"
    elsif os == OS_MACOSX
      warning "Mac OS X is only provisionally supported"
    end

    # Architecture is unknown by default.
    debug "Checking processor architecture" 
    uname_m = cmd_result("uname -m")
    arch = case
    when uname_m == "x86_64" then OS_ARCH_64
    when uname_m == "i386" then OS_ARCH_32
    when uname_m == "i686" then OS_ARCH_32
    else
      OS_ARCH_UNKNOWN
    end
    if arch == OS_ARCH_UNKNOWN
      raise "Could not determine OS architecture.  The `uname -m` response does not match \"x86_64\", \"i386\" or \"i686\""
    elsif arch == OS_ARCH_32
      warning "32-bit architecture not recommended for DBMS nodes"
    end

    # Report on Linux distribution.
    if os == OS_LINUX
      debug "Checking Linux distribution" 
      if File.exist?("/etc/redhat-release")
        system = cmd_result("cat /etc/redhat-release")
      elsif File.exist?("/etc/debian_version")
        system = cmd_result("cat /etc/debian_version")
      else
        debug "Tungsten checks for the presence of \"/etc/redhat-release\" or \"/etc/debian_version\" to determine the distribution." 
        raise "Could not determine Linux distribution.  Tungsten has been tested on RedHat and Debian systems."
      end
    end

    debug "Supported operating system found: #{system}"
  end
end

class JavaVersionCheck < ConfigureValidationCheck
  def set_vars
    @title = "Java version"
  end
  
  def validate
    # Look for Java.
    java_out = cmd_result("java -version")
    if $? == 0
      if java_out =~ /Java|JDK/
        debug "Supported Java found"
        
        java_version = java_out.scan(/java version \"1.(6|7|8)./)
        unless java_version.length == 1
          error "Java 1.6 or greater is required to run Tungsten"
        end
      else
        error "Unknown Java version"
      end
    else
      error "Java binary not found in path"
    end
  end
end

class SudoCheck < ConfigureValidationCheck
  def set_vars
    @title = "Sudo"
  end
  
  def validate
    sudo_ls_output = cmd_result("sudo -n ls", true)
    if $? != 0
      error "Sudo is not setup correctly"
      add_help()
    else
      sudo_output = cmd_result("sudo -l")
      
      if sudo_output =~ /requiretty/
        error "Sudo has the requiretty option enabled"
      end
      unless sudo_output =~ /NOPASSWD: ALL/
        error "The user does not have access to sudo all commands."
      end
      
      if is_valid?()
        debug "Sudo access is setup correctly"
      else
        add_help()
      end
    end
  end
  
  def add_help
    help("Add \"#{@config.getProperty(USERID)}        ALL=(ALL)       NOPASSWD: ALL\" to the /etc/sudoers file.")
    help("Comment out or remove the requiretty line in the /etc/sudoers file.")
  end
  
  def enabled?
    (@config.getProperty(ROOT_PREFIX) == "true")
  end
end

class HostnameCheck < ConfigureValidationCheck
  def set_vars
    @title = "Hostname"
    @description = "Ensure hostname is legal host name, not localhost"
  end

  # Check the host name. 
  def validate
    unless Configurator.instance.is_localhost?(@config.getProperty(HOST))
      error "Hostname must be #{@config.getProperty(HOST)}"
    else
      debug "Hostname is OK"
    end
  end
end

class PackageDownloadCheck < ConfigureValidationCheck
  def set_vars
    @title = "Package download check"
  end
  
  def validate
    if @config.getProperty(DEPLOY_PACKAGE_URI) != nil
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))
      if uri.scheme == "http" || uri.scheme == "https"
        success_lines_count = cmd_result("curl -I -s -k #{@config.getProperty(DEPLOY_PACKAGE_URI)} | grep HTTP | grep 200 | wc -l")
        if success_lines_count.to_i() == 1
          info("The package download link is accessible")
        else
          error("The package download link is not accessible")
        end
      end
    end
  end
end

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
    (@config.getProperty(SVC_INSTALL) == "true")
  end
end

class ClusterSSHLoginCheck < ConfigureValidationCheck
  def set_vars
    @title = "SSH login check"
  end
  
  def validate
    @config.getProperty(REPL_HOSTS).split(",").each{
      |repl_host|
      remote_user = ssh_result("echo $USER", repl_host, @config.getProperty(USERID))
      if remote_user != @config.getProperty(USERID)
        error("SSH login failed from #{@config.getProperty(HOST)} to #{repl_host}")
      else
        info("SSH login successful from #{@config.getProperty(HOST)} to #{repl_host}")
      end
    }
  end
  
  def enabled?
    # Disabled until the manager is present and needs SSH access between hosts
    false
  end
end