module ConfigureDeploymentStepManager
  DEFAULT_MANAGER_PING_PORT = "7800"
  
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_manager"),
      ConfigureDeploymentMethod.new("create_cluster_home_directories", -1)
    ]
  end
  module_function :get_deployment_methods
  
  # Configure the Tungsten Manager
  def deploy_manager
    unless is_manager?() then
      info "Tungsten Manager is not required on this host because"
      info "the replicator is not enabled and the router is using the new protocol."
      return
    end
    
    Configurator.instance.write_header "Performing Tungsten Manager configuration..."
    
    write_manager_properties()
    write_replicator_manager_extension_properties()

    case @config.getProperty(GLOBAL_GC_MEMBERSHIP)
    when "gossip"
      # Setup group discovery via Gossip router.

      # Construct a host list for port numbers on each host.
      initial_hosts = fill_ports_near_hosts(@config.getProperty(GLOBAL_HOSTS), get_manager_gossip_port())

      # Fill config files.
      gossip_xml = "/jgroups_tcp_gossip.xml"
      apply_config_hedera("#{get_deployment_basedir()}/tungsten-manager/conf/hedera.properties", gossip_xml)
      apply_config_gossip_jgroups("#{get_deployment_basedir()}/tungsten-manager/conf" + gossip_xml, initial_hosts)
      apply_config_gossip_router()
    when "multicast"
      # Setup group discovery via MULTICAST.
      apply_config_multicast_jgroups("#{get_deployment_basedir()}/tungsten-manager/conf/jgroups_tcp.xml")
    else # ping
      # Setup group discovery via TCPPING.

      # Construct a host list for port numbers on each host.
      initial_hosts = fill_ports_near_hosts(@config.getProperty(GLOBAL_HOSTS), get_manager_ping_port())

      # Fill config files.
      ping_xml = "/jgroups_tcp_ping.xml"
      apply_config_hedera("#{get_deployment_basedir()}/tungsten-manager/conf/hedera.properties", ping_xml)
      apply_config_ping_jgroups("#{get_deployment_basedir()}/tungsten-manager/conf" + ping_xml, initial_hosts)
    end
    
    # Configure user name in service script.
    set_run_as_user("#{get_deployment_basedir()}/tungsten-manager/bin/manager")

    # Register manager service.
    add_service("tungsten-manager/bin/manager")
    
    FileUtils.cp("#{get_deployment_basedir()}/tungsten-manager/conf/manager.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + @config.getProperty(GLOBAL_DSNAME) + "/service/manager.properties")
  end
  
  def write_manager_properties
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/tungsten-manager/conf/sample.manager.properties",
      "#{get_deployment_basedir()}/tungsten-manager/conf/manager.properties", "# ")

    transformer.transform { |line|
      if line =~ /manager.gc.default_join/ then
        "manager.gc.default_join=true"
      elsif line =~ /manager.gc.group/ then
        "manager.gc.group=" + @config.getProperty(GLOBAL_DSNAME)
      elsif line =~ /manager.gc.member/ then
        "manager.gc.member=" + @config.getProperty(GLOBAL_HOST)
      elsif line =~ /manager.cluster.policy/ then
        "manager.cluster.policy=com.continuent.tungsten.cluster.manager.policy.EnterprisePolicyManager"
      elsif line =~ /manager.policy.mode/ then
        "manager.policy.mode=" + @config.getProperty(POLICY_MGR_MODE)
      elsif line =~ /manager.monitor.start/ then
        unless enable_external_monitor?()  then
          "manager.monitor.start=true"
        else
          "manager.monitor.start=false"
        end
      elsif line =~ /manager.replicator.proxy/ then
        # Hack to get correct JMX interface handler.  Should be in
        # subconfigurator.
        "manager.replicator.proxy=com.continuent.tungsten.manager.resource.proxy.ReplicatorManagerProxyImplV2"
      elsif line =~ /vip.isEnabled/ then
        if @config.getProperty(REPL_MASTER_VIP) != "" && @config.getProperty(REPL_MASTER_VIP) != "none" then
          "vip.isEnabled=true"
        else
          "vip.isEnabled=false"
        end
      elsif line =~ /vip.interface/ then
        "vip.interface=#{@config.getProperty(REPL_MASTER_VIP_DEVICE)}"
      elsif line =~ /vip.address/ then
        "vip.address=#{@config.getProperty(REPL_MASTER_VIP)}"
      elsif line =~ /vip.ifconfig_path/ then
        "vip.ifconfig_path=#{@config.getProperty(REPL_MASTER_VIP_IFCONFIG)}"
      elsif line =~ /manager.sudoCommandPrefix/ then
        "manager.sudoCommandPrefix=#{get_root_prefix()}"
      elsif line =~ /manager.global.witnesses/ then
        "manager.global.witnesses=#{@config.getProperty(GLOBAL_WITNESSES)}"
      elsif line =~ /manager.global.members/ then
        "manager.global.members=#{@config.getProperty(GLOBAL_HOSTS)}"
      elsif line =~ /manager.readOnlySlaves/ then
        "manager.readOnlySlaves=#{@config.getProperty(REPL_MYSQL_RO_SLAVE)}"
      elsif line =~ /policy.liveness.dsPingTimeout/ then
        "policy.liveness.dsPingTimeout=#{@config.getProperty(MGR_DB_PING_TIMEOUT)}"
      elsif line =~ /policy.liveness.hostPingTimeout/ then
        "policy.liveness.hostPingTimeout=#{@config.getProperty(MGR_HOST_PING_TIMEOUT)}"
      elsif line =~ /policy.liveness.samplePeriodSecs/ then
        "policy.liveness.samplePeriodSecs=#{@config.getProperty(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_SECS)}"
      elsif line =~ /policy.liveness.thresholdPeriods/ then
        "policy.liveness.thresholdPeriods=#{@config.getProperty(MGR_POLICY_LIVENESS_SAMPLE_PERIOD_THRESHOLD)}"
      elsif line =~ /policy.fence.slaveReplicator/ then
        "policy.fence.slaveReplicator=#{@config.getProperty(MGR_POLICY_FENCE_SLAVE_REPLICATOR)}"
      elsif line =~ /policy.fence.masterReplicator/ then
        "policy.fence.masterReplicator=#{@config.getProperty(MGR_POLICY_FENCE_MASTER_REPLICATOR)}"
      elsif line =~ /manager.notifications.timeout/ then
        "manager.notifications.timeout=#{@config.getProperty(MGR_NOTIFICATIONS_TIMEOUT)}"
      elsif line =~ /manager.notifications.send/ then
        "manager.notifications.send=#{@config.getProperty(MGR_NOTIFICATIONS_SEND)}"
      elsif line =~ /manager.idle.router.timeout/ then
        "manager.idle.router.timeout=#{@config.getProperty(MGR_IDLE_ROUTER_TIMEOUT)}"
      elsif line =~ /manager.fail.threshold/ then
        "manager.fail.threshold=#{@config.getProperty(MGR_POLICY_FAIL_THRESHOLD)}"
      else
        line
      end
    }
  end
  
  def apply_config_hedera(hedera_properties, jgroups_xml)
    transformer = Transformer.new(
    hedera_properties,
    hedera_properties, nil)
    transformer.transform { |line|
      if line =~ /hedera.channel.jgroups.config/ then
        "hedera.channel.jgroups.config=" + jgroups_xml
      else
        line
      end
    }
  end

  def apply_config_gossip_jgroups(jgroups_xml, initial_hosts)
    transformer = Transformer.new(jgroups_xml, jgroups_xml, nil)
    transformer.transform { |line|
      if line =~ /<TCPGOSSIP initial_hosts/ then
        "<TCPGOSSIP initial_hosts=\"" + initial_hosts + "\""
      elsif line =~ /<TCP bind_addr="([0-9.]*)"/ then
        line.sub $1, @config.getProperty(GLOBAL_IP_ADDRESS)
      else
        line
      end
    }
  end

  def apply_config_ping_jgroups(jgroups_xml, initial_hosts)
    transformer = Transformer.new(jgroups_xml, jgroups_xml, nil)
    transformer.transform { |line|
      if line =~ /<TCPPING initial_hosts/ then
        "<TCPPING initial_hosts=\"" + initial_hosts + "\""
      elsif line =~ /<TCP bind_addr="([0-9.]*)"/ then
        line.sub $1, @config.getProperty(GLOBAL_IP_ADDRESS)
      else
        line
      end
    }
  end

  def apply_config_multicast_jgroups(jgroups_xml)
    transformer = Transformer.new(jgroups_xml, jgroups_xml, nil)
    transformer.transform { |line|
      if line =~ /<TCP bind_addr="([0-9.]*)"/ then
        line.sub $1, @config.getProperty(GLOBAL_IP_ADDRESS)
      else
        line
      end
    }
  end
  
  def apply_config_hedera_monitoring(hedera_monitoring_properties)
    transformer = Transformer.new(hedera_monitoring_properties, 
      hedera_monitoring_properties, nil)

    transformer.transform { |line|
      if line =~ /hedera.channel.name=/
        "hedera.channel.name=" + @config.getProperty(GLOBAL_DSNAME) + ".monitoring"
      else
        line
      end
    }
  end

  # Apply configuration for gossip router.
  def apply_config_gossip_router
    if @config.getProperty(GLOBAL_GC_MEMBERSHIP) != "gossip" then
      info "Gossip protocol is not active; skipping Gossip Router configuration"
      return
    end

    Configurator.instance.write_header "Performing Gossip Router configuration..."

    # Fix up the wrapper.conf file.
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/gossiprouter/conf/gossipwrapper.conf",
      "#{get_deployment_basedir()}/gossiprouter/conf/gossipwrapper.conf", nil)

    transformer.transform { |line|
      if line =~ /wrapper.app.parameter.3/
        # Set the gossip router port correctly.
        "wrapper.app.parameter.3=" + @config.getProperty(GLOBAL_GOSSIP_PORT)
      else
        line
      end
    }

    # Configure user name in service script.
    set_run_as_user("#{get_deployment_basedir()}/gossiprouter/bin/gossiprouter")

    # Perform service configuration for gossip router.
    add_service("gossiprouter/bin/gossiprouter")
    FileUtils.cp(
      "#{get_deployment_basedir()}/gossiprouter/conf/gossiprouter.service.properties", 
      "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + @config.getProperty(GLOBAL_DSNAME) + "/service/gossiprouter.properties")
  end
  
  def create_cluster_home_directories
    # Create data service directory.
    debug("Creating directories for services and data sources")
    cmd_result("mkdir -p #{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(GLOBAL_DSNAME)}/datasource")
    cmd_result("mkdir -p #{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(GLOBAL_DSNAME)}/service")
    cmd_result("mkdir -p #{get_deployment_basedir()}/cluster-home/conf/cluster/#{@config.getProperty(GLOBAL_DSNAME)}/extension")
  end
  
  # Generate file that has default monitoring hooks
  def write_replicator_manager_extension_properties()
    cluster = @config.getProperty(GLOBAL_DSNAME)
    prefix = get_root_prefix()
    props_name = "event.properties"
    svc_properties_dir = "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + cluster + "/extension/"
    svc_properties = svc_properties_dir + "/" + props_name
    if prefix == ""
      prefix_to_append = ""
    else
      prefix_to_append = prefix + " "
    end

    # Ensure services properties directory exists.
    `mkdir -p #{svc_properties_dir}`

    # Create service properties file.
    out = File.open(svc_properties, "w")
    out.puts("# event.properties")
    out.puts("name=event")
    out.puts("command.onResourceStateTransition=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onDataSourceStateTransition=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onFailover=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onPolicyAction=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onRecovery=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onDataSourceCreate=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts("command.onResourceNotification=#{prefix_to_append}${manager.home}/scripts/echoEvent.sh")
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + svc_properties
  end
  
  def is_manager?
    (is_replicator?() || @config.getProperty(SQLR_USENEWPROTOCOL) != "true")
  end
  
  def get_manager_ping_port
    DEFAULT_MANAGER_PING_PORT
  end
  
  def get_manager_gossip_port
    @config.getProperty(GLOBAL_GOSSIP_PORT)
  end
end