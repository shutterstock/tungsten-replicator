module ConfigureDeploymentStepReplicationDataservice
  def get_deployment_methods
    [
    ]
  end
  module_function :get_deployment_methods
  
  def deploy_replication_dataservice()
    mkdir_if_absent(@config.getProperty(get_service_key(REPL_LOG_DIR)))
    
    if @config.getProperty(get_service_key(REPL_RELAY_LOG_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_RELAY_LOG_DIR)))
    end
    
    if @config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR))
      mkdir_if_absent(@config.getProperty(get_service_key(REPL_BACKUP_STORAGE_DIR)))
    end
    
    # Configure replicator.properties.service.template
    transformer = Transformer.new(
		  get_replication_dataservice_template(),
			@config.getProperty(REPL_SVC_CONFIG_FILE), "#")
		
		transformer.set_fixed_properties(@config.getProperty(get_service_key(FIXED_PROPERTY_STRINGS)))
		transformer.transform_values(method(:transform_replication_dataservice_values))
    transformer.output
		
		if @config.getProperty(REPL_SVC_REPORT) == "true" || @config.getProperty(REPL_SVC_START) == "true"
		  if svc_is_running?(get_svc_command("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator"))
		    begin
		      info("Check if the replication service is running")
		      cmd_result("#{get_trepctl_cmd()} -service #{@config.getProperty(get_service_key(DEPLOYMENT_SERVICE))} status")
		      warning("The replication service is already running.  You must stop and start it for the changes to take effect.")
        rescue CommandError => ce
          # The status command fails if the service is not running
          info("Start the replication service")
          cmd_result("#{get_trepctl_cmd()} -service #{@config.getProperty(get_service_key(DEPLOYMENT_SERVICE))} start")
          
          if @config.getProperty(REPL_SVC_REPORT) == "true"
            output("Getting services list")
            services = cmd_result("#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)} services")
            output(services)
          end
        end
      else
        info("Start the replicator")
        self.trigger_event(:before_service_start, 'replicator')
        cmd_result(get_svc_command("#{get_deployment_basedir()}/tungsten-replicator/bin/replicator start"))
        self.trigger_event(:after_service_start, 'replicator')
        
        if @config.getProperty(REPL_SVC_REPORT) == "true"
          output("Getting services list")
          services = cmd_result("#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)} services")
          output(services)
        end
      end
    end
  end
	
	def transform_replication_dataservice_values(matches)
	  case matches.at(0)
    when "APPLIER"
      v = @config.getTemplateValue(get_applier_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    when "EXTRACTOR"
      v = @config.getTemplateValue(get_extractor_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    when "SERVICE"
      v = @config.getTemplateValue(get_service_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    when "HOST"
      v = @config.getTemplateValue(get_host_key(Kernel.const_get(matches[1])), method(:transform_replication_dataservice_values))
    else
      v = @config.getTemplateValue(matches.map{
        |match|
        Kernel.const_get(match)
      }, method(:transform_replication_dataservice_values))
    end
    
    return v
	end
	
	def get_replication_dataservice_template()
	  pipelines = @config.getProperty(REPL_PIPELINES).tr_s(',', '')
	  template = "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/replicator.properties.#{pipelines}"
	  if File.exist?(template)
	    return template
	  else
      raise "Unable to determine the replication service template file"
    end
	end
	
	def get_applier_datasource()
	  ds = @config.getProperty(REPL_DATASOURCE)
    if ds.to_s() == ""
      ds = DEFAULTS
    end
    
    ConfigureDatabasePlatform.build(
      @config.getProperty([DATASOURCES, ds, REPL_DBTYPE]),
      @config.getProperty([DATASOURCES, ds, REPL_DBHOST]),
      @config.getProperty([DATASOURCES, ds, REPL_DBPORT]),
      @config.getProperty([DATASOURCES, ds, REPL_DBLOGIN]),
      @config.getProperty([DATASOURCES, ds, REPL_DBPASSWORD]), @config)
  end
  
  def get_extractor_datasource()
    if @config.getProperty(REPL_ROLE) == REPL_ROLE_DI
      ds = @config.getProperty(REPL_MASTER_DATASOURCE)
      if ds.to_s() == ""
        ds = DEFAULTS
      end

      ConfigureDatabasePlatform.build(
        @config.getProperty([DATASOURCES, ds, REPL_DBTYPE]),
        @config.getProperty([DATASOURCES, ds, REPL_DBHOST]),
        @config.getProperty([DATASOURCES, ds, REPL_DBPORT]),
        @config.getProperty([DATASOURCES, ds, REPL_DBLOGIN]),
        @config.getProperty([DATASOURCES, ds, REPL_DBPASSWORD]), @config)
    else
      get_applier_datasource()
    end
  end
  
  def get_service_key(key)
    [REPL_SERVICES, @config.getProperty(DEPLOYMENT_SERVICE), key]
  end
  
  def get_applier_key(key)
    [DATASOURCES, @config.getProperty(REPL_DATASOURCE), key]
  end
  
  def get_extractor_key(key)
    if @config.getProperty(REPL_ROLE) == REPL_ROLE_DI
      [DATASOURCES, @config.getProperty(REPL_MASTER_DATASOURCE), key]
    else
      get_applier_key(key)
    end
  end
  
  def get_trepctl_cmd
    "#{get_deployment_basedir()}/tungsten-replicator/bin/trepctl -port #{@config.getProperty(REPL_RMI_PORT)}"
  end
end