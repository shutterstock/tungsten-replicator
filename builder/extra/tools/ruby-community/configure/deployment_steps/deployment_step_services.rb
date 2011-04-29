module ConfigureDeploymentStepServices
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("apply_config_services", ConfigureDeployment::FINAL_STEP_WEIGHT),
    ]
  end
  module_function :get_deployment_methods
  
  # Set up files and perform other configuration for services.
  def apply_config_services
    Configurator.instance.write_header "Performing services configuration"

    config_wrapper()
    
    if @config.getProperty(SVC_INSTALL) == "true" then
      write_deployall()
      write_undeployall()
      write_startallsvcs()
      write_stopallsvcs()
            
      info("Installing services")
      installed = cmd_result("#{get_root_prefix()} #{get_deployment_basedir()}/cluster-home/bin/deployall")
      info(installed)
    else
      write_startall()
      write_stopall()
    end
    
    if @config.getProperty(SVC_START) == "true"
      info("Starting services")
      started = cmd_result("#{get_deployment_basedir()}/cluster-home/bin/startall")
      info(started)
    end
  end
  
  def config_wrapper
    # Patch for Ubuntu 64-bit start-up problem.
    if Configurator.instance.distro?() == OS_DISTRO_DEBIAN && Configurator.instance.arch?() == OS_ARCH_64
      wrapper_file = "#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32"
      if File.exist?(wrapper_file)
        FileUtils.rm("#{get_deployment_basedir()}/cluster-home/bin/wrapper-linux-x86-32")
      end
    end
  end
  
  def write_startall
    # Create startall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/startall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Start all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.each { |svc| out.puts get_svc_command(svc + " start") }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_startallsvcs
    # Create startallsvcs script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/startall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Start all services"
      @services.each { |svc|
        svcname = File.basename svc
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/service t" + svcname + " start")
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("/etc/init.d/t" + svcname + " start")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end

  def write_stopall
    # Create stopall script.
    script = "#{get_deployment_basedir()}/cluster-home/bin/stopall"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Stop all services using local service scripts"
    out.puts "THOME=`dirname $0`/../.."
    out.puts "cd $THOME"
    @services.reverse_each { |svc| out.puts get_svc_command(svc + " stop") }
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_stopallsvcs
    # Create stopallsvcs script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/stopall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Stop all services"
      @services.reverse_each { |svc|
        svcname = File.basename svc
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/service t" + svcname + " stop")
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("/etc/init.d/t" + svcname + " stop")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end

  def write_deployall
    # Create deployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/deployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Install services into /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        out.puts get_svc_command("ln -fs $PWD/" + svc + " /etc/init.d/t" + svcname)
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --add t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("update-rc.d t" + svcname + " defaults")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end

  def write_undeployall
    # Create undeployall script.
    if Configurator.instance.can_install_services_on_os?()
      script = "#{get_deployment_basedir()}/cluster-home/bin/undeployall"
      out = File.open(script, "w")
      out.puts "#!/bin/bash"
      out.puts "# Remove services from /etc directories"
      out.puts "THOME=`dirname $0`/../.."
      out.puts "cd $THOME"
      @services.each { |svc|
        svcname = File.basename svc
        if Configurator.instance.distro?() == OS_DISTRO_REDHAT
          out.puts get_svc_command("/sbin/chkconfig --del t" + svcname)
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
        elsif Configurator.instance.distro?() == OS_DISTRO_DEBIAN
          out.puts get_svc_command("rm -f /etc/init.d/t" + svcname)
          out.puts get_svc_command("update-rc.d -f  t" + svcname + " remove")
        end
      }
      out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
      out.chmod(0755)
      out.close
      info "GENERATED FILE: " + script
    end
  end
end