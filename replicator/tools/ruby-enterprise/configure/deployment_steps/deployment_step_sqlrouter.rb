module ConfigureDeploymentStepSqlRouter
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_sqlrouter")
    ]
  end
  module_function :get_deployment_methods
  
  # Configure the Tungsten SQL Router
  def deploy_sqlrouter
    Configurator.instance.write_header("Performing Tungsten SQL Router configuration...")

    write_router_properties()
    write_policymgr_properties()
  end
  
  def write_router_properties
    # Write the router.properties file for general configuration.
    if File.exists?("#{get_deployment_basedir()}/tungsten-sqlrouter/conf/sample.router.properties")
      sql_router_properties_file = "#{get_deployment_basedir()}/tungsten-sqlrouter/conf/sample.router.properties"
    else
      sql_router_properties_file = "#{get_deployment_basedir()}/tungsten-sqlrouter/conf/router.properties"
    end
    
    transformer = Transformer.new(
      sql_router_properties_file,
      "#{get_deployment_basedir()}/cluster-home/conf/router.properties", "# ")

    transformer.transform { |line|
      if line =~ /^driverListenerClass/ then
        "driverListenerClass=com.continuent.tungsten.router.adaptor.DriverNotificationListener"
      elsif line =~ /^routerListenerClass/ then
        "routerListenerClass=com.continuent.tungsten.router.adaptor.DriverNotificationListener"
      elsif line =~ /^clusterName/ then
        "clusterName=" +  @config.getProperty(GLOBAL_DSNAME)
      elsif line =~ /^waitForDisconnectTimeout/ then
        "waitForDisconnectTimeout=" +  @config.getProperty(ROUTER_WAITFOR_DISCONNECT)
      elsif line =~ /^delayBeforeOfflineIfNoManager/ then
        "delayBeforeOfflineIfNoManager=" + @config.getProperty(SQLR_DELAY_BEFORE_OFFLINE)
      elsif line =~ /^keepAliveTimeout/ then
        "keepAliveTimeout=" + @config.getProperty(SQLR_KEEP_ALIVE_TIMEOUT)
      elsif line =~ /^clusterMemberName/ then
        "clusterMemberName=" +  @config.getProperty(GLOBAL_HOST)
        # FIXME: get manager hostname when possible
      elsif line =~ /^useNewProtocol/ then
        "useNewProtocol=" + @config.getProperty(SQLR_USENEWPROTOCOL)
      elsif line =~ /^managerList/ then
        "managerList=" + @config.getProperty(GLOBAL_HOSTS)
      else
        line
      end
    }
    
    # Write the same router.properties file to the bristlecone configuration.
    FileUtils.cp("#{get_deployment_basedir()}/cluster-home/conf/router.properties", 
      "#{get_deployment_basedir()}/bristlecone/config/")
    FileUtils.cp("#{get_deployment_basedir()}/cluster-home/conf/router.properties", 
      "#{get_deployment_basedir()}/tungsten-sqlrouter/conf/")
  end
  
  def write_policymgr_properties
    # Write the policymgr.properties file.
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-sqlrouter/conf/policymgr.properties",
      "#{get_deployment_basedir()}/cluster-home/conf/policymgr.properties", "# ")

    transformer.transform { |line|
      if line =~ /^notifierMonitorClass/ then
        "notifierMonitorClass=com.continuent.tungsten.commons.patterns.notification.adaptor.MonitorNotifierGroupCommAdaptor"
      else
        line
      end
    }
  end
end