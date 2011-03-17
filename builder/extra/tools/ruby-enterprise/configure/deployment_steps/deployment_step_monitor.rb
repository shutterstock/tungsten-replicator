module ConfigureDeploymentStepMonitor
  REPLICATOR_CHECKERFILE = "checker.tungstenreplicator.properties"
  
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_monitor")
    ]
  end
  module_function :get_deployment_methods
  
  # Perform generic monitor configuration.
  def deploy_monitor
    unless is_monitor?() then
      info "Monitor is not active; skipping configuration"
      return
    end

    Configurator.instance.write_header "Performing Tungsten Monitor configuration..."
    
    write_monitor_properties()
    write_monitor_checker_replicator()

    # Configure user name in service script.
    set_run_as_user("#{get_deployment_basedir()}/tungsten-monitor/bin/monitor")

    # Register the monitor service.
    if enable_external_monitor?() then
      add_service("tungsten-monitor/bin/monitor")
    end
    
    FileUtils.cp(
      "#{get_deployment_basedir()}/tungsten-monitor/conf/monitor.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + @config.getProperty(GLOBAL_DSNAME) + "/service/monitor.properties")
  end
  
  def write_monitor_properties
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-monitor/conf/sample.monitor.properties",
      "#{get_deployment_basedir()}/tungsten-monitor/conf/monitor.properties", "# ")

    transformer.transform { |line|
      if line =~ /check.frequency.ms/
        "check.frequency.ms=" + @config.getProperty(REPL_MONITOR_INTERVAL)
      elsif line =~ /cluster.name/
        "cluster.name=" + @config.getProperty(GLOBAL_DSNAME)
      elsif line =~ /cluster.member/
        "cluster.member=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /notifier.gcnotifier.channelName=/
        "notifier.gcnotifier.channelName=" + @config.getProperty(GLOBAL_DSNAME) + ".monitoring"
      else
        line
      end
    }
  end
  
  def write_monitor_checker_replicator
    # Configure monitoring for regular Tungsten Replicator.
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-monitor/conf/sample.#{REPLICATOR_CHECKERFILE}",
      "#{get_deployment_basedir()}/tungsten-monitor/conf/#{REPLICATOR_CHECKERFILE}", "# ")

    transformer.transform { |line|
      if line =~ /frequency=/
        "frequency=" + @config.getProperty(MON_REPLICATOR_CHECK_FREQUENCY)
      elsif line =~ /sourceId=/
        "sourceId=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /clusterName=/
        "clusterName=" + @config.getProperty(GLOBAL_DSNAME)
      else
        line
      end
    }
  end
  
  def enable_external_monitor?
    if is_replicator?() && 
        @config.getProperty(OPTION_MONITOR_INTERNAL) =~ /true/
      false
    else
      true
    end
  end
  
  def is_monitor?
    is_replicator?()
  end
end