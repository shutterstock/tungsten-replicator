module ConfigureDeploymentStepDeployment
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("build_home_directory", -40),
      ConfigureDeploymentMethod.new("create_release", -30),
      ConfigureDeploymentMethod.new("deploy_config_files", -20),
    ]
  end
  module_function :get_deployment_methods
  
  def build_home_directory
    Configurator.instance.write_header("Building the Tungsten home directory")
    mkdir_if_absent("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/configs")
    mkdir_if_absent("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/service-logs")
    mkdir_if_absent("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases")
    mkdir_if_absent("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/share")

    # Create share/env.sh script.
    script = "#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/share/env.sh"
    out = File.open(script, "w")
    out.puts "# Source this file to set your environment."
    out.puts "export TUNGSTEN_HOME=#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}"
    out.puts "export PATH=$TUNGSTEN_HOME/#{Configurator::CURRENT_RELEASE_DIRECTORY}/tungsten-manager/bin:$TUNGSTEN_HOME/#{Configurator::CURRENT_RELEASE_DIRECTORY}/tungsten-replicator/bin:$PATH"
    out.chmod(0755)
    out.close
    debug("Generate environment at #{script}")
  end
  
  def create_release
    if @config.getProperty(DEPLOY_CURRENT_PACKAGE) == "true"
      debug("Copy #{Configurator.instance.get_base_path()} to #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases")
      cmd_result("cp -rf #{Configurator.instance.get_base_path()} #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases")
      
      package_basename = Configurator.instance.get_basename()
    else
      debug("Download and unpack #{@config.getProperty(DEPLOY_PACKAGE_URI)}")
      uri = URI::parse(@config.getProperty(DEPLOY_PACKAGE_URI))

      if uri.scheme == "http" || uri.scheme == "https"
        unless @config.getProperty(DEPLOY_PACKAGE_URI) =~ /.tar.gz/
          raise "Only files ending in .tar.gz may be fetched using #{uri.scheme.upcase}"
        end

        package_basename = File.basename(@config.getProperty(DEPLOY_PACKAGE_URI), ".tar.gz")
        unless (File.exists?("#{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}.tar.gz"))
          cmd_result("cd #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}; wget --no-check-certificate #{@config.getProperty(DEPLOY_PACKAGE_URI)}")
        else
          debug("Using the package already downloaded to #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
        end

        cmd_result("cd #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases; tar zxf #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
      elsif uri.scheme == "file"
        rsync_cmd = ["rsync"]
      
        unless uri.port
          rsync_cmd << "-aze ssh --delete"
        else
          rsync_cmd << "-aze \"ssh --delete -p #{uri.port}\""
        end
      
        if uri.host != "localhost"
          unless uri.userinfo
            rsync_cmd << "#{uri.host}:#{uri.path}"
          else
            rsync_cmd << "#{uri.userinfo}@#{uri.host}:#{uri.path}"
          end

          rsync_cmd << @config.getProperty(GLOBAL_TEMP_DIRECTORY)
        
          cmd_result(rsync_cmd.join(" "))
        else
          unless File.dirname(uri.path) == @config.getProperty(GLOBAL_TEMP_DIRECTORY)
            cmd_result("cp #{uri.path} #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}")
          end
        end
        
        package_basename = File.basename(uri.path)
        if package_basename =~ /.tar.gz$/
          package_basename = File.basename(package_basename, ".tar.gz")
          
          cmd_result("cd #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases; tar zxf #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}.tar.gz")
        elsif package_basename =~ /.tar$/
          package_basename = File.basename(package_basename, ".tar")
          
          cmd_result("cd #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases; tar xf #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}.tar")
        elsif File.directory?("#{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename}")
          cmd_result("cp -rf #{@config.getProperty(GLOBAL_TEMP_DIRECTORY)}/#{package_basename} #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases")
        else
          raise "#{package_basename} is not a directory or recognized archive file"
        end
      else
        raise "Unable to download package from #{@config.getProperty(DEPLOY_PACKAGE_URI)}: #{uri.scheme.upcase()} is an unrecognized scheme"
      end
    end
    
    debug("Create symlink to #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases/#{package_basename}")
    cmd_result("rm -f #{get_deployment_basedir()}; ln -s #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/releases/#{package_basename} #{get_deployment_basedir()}")
    
    # Remove any copied config files to keep the release directory clean
    FileUtils.rm_f("#{get_deployment_basedir()}/#{Configurator::CLUSTER_CONFIG}")
    FileUtils.rm_f("#{get_deployment_basedir()}/#{Configurator::TEMP_DEPLOY_HOST_CONFIG}")
    FileUtils.rm_f("#{get_deployment_basedir()}/#{Configurator::TEMP_DEPLOY_CLUSTER_CONFIG}")
  end
  
  def deploy_config_files
    debug("Copy #{Configurator.instance.get_config_filename()} to #{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/configs/#{Configurator::HOST_CONFIG}")
    FileUtils.cp("#{Configurator.instance.get_config_filename()}", "#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/configs/#{Configurator::HOST_CONFIG}", :preserve => true)

    host_config = Properties.new()
    host_config.load("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/configs/#{Configurator::HOST_CONFIG}")
    host_config.setProperty(GLOBAL_DEPLOYMENT_TYPE, "direct")
    host_config.setProperty(DEPLOY_PACKAGE_URI, nil)
    host_config.setProperty(GLOBAL_DEPLOY_PACKAGE_URI, nil)
    host_config.setProperty(DEPLOY_CURRENT_PACKAGE, nil)
    host_config.store("#{@config.getProperty(GLOBAL_HOME_DIRECTORY)}/configs/#{Configurator::HOST_CONFIG}")
  end
end