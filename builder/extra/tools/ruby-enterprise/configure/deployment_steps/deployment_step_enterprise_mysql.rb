system_require "configure/deployment_steps/deployment_step_enterprise_replicator"
module ConfigureDeploymentStepEnterpriseMySQL
  include ConfigureDeploymentStepEnterpriseReplicator
  
  def apply_config_replicator
    super()
    #deploy_mysql_readonly_service()
    #write_monitor_checker_mysql()
  end
  
  def deploy_mysql_readonly_service
	  # Create service properties for mysql_readonly script.
		service_mysql_ro = "#{get_deployment_basedir()}/tungsten-cluster-manager/rules-ext/mysql_readonly.service.properties"
		if (File.exist?(service_mysql_ro))
			FileUtils.cp(service_mysql_ro, 
			  "#{get_deployment_basedir()}/cluster-home/conf/cluster/" + @config.getProperty(GLOBAL_DSNAME) + "/service/mysql_readonly.properties")
		end
		
		# Generate the MySQL read-only script.  This must have the Tungsten
		# admin user and password.
		user = @config.getProperty(REPL_DBLOGIN)
		pw   = @config.getProperty(REPL_DBPASSWORD)
		script = "#{get_deployment_basedir()}/cluster-home/bin/mysql_readonly"
		out = File.open(script, "w")
		out.puts "#!/bin/bash"
		out.puts "# Make MySQL read-only"
		out.puts "mysql -h#{@config.getProperty(GLOBAL_HOST)} --port=#{@config.getProperty(REPL_DBPORT)} -u#{user} -p#{pw} -e \"SET GLOBAL read_only = $1;\""
		out.puts "mysql -h#{@config.getProperty(GLOBAL_HOST)} --port=#{@config.getProperty(REPL_DBPORT)} -u#{user} -p#{pw} -e \"SHOW VARIABLES LIKE '%read_only%';\""
		out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
		out.chmod(0755)
		out.close
		info("GENERATED FILE: " + script)
	end
	
	def write_monitor_checker_mysql
	  # Configure monitoring for MySQL.
		transformer = Transformer.new(
									  "#{get_deployment_basedir()}/tungsten-monitor/conf/sample.checker.mysqlserver.properties",
									  "#{get_deployment_basedir()}/tungsten-monitor/conf/checker.mysqlserver.properties", "# ")
		
		user = @config.getProperty(REPL_DBLOGIN)
		password = @config.getProperty(REPL_DBPASSWORD)
		
		transformer.transform { |line|
			if line =~ /serverName=/
				"serverName=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /url=/
				"url=jdbc:mysql://" + @config.getProperty(GLOBAL_HOST) + ':' + @config.getProperty(REPL_DBPORT) + "?jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&allowMultiQueries=true&yearIsDateType=false"
			elsif line =~ /frequency=/
				"frequency=" + @config.getProperty(MON_DB_CHECK_FREQUENCY)
			elsif line =~ /host=/
				"host=" + @config.getProperty(GLOBAL_HOST)
			elsif line =~ /username=/
				"username=" + user
			elsif line =~ /queryTimeout=/
        "queryTimeout=#{@config.getProperty(MON_DB_QUERY_TIMEOUT)}"
			elsif line =~ /password=/
				if password == "" || password == nil then
					"password="
				else
					"password=" + password
				end
			else
				line
			end
		}
	end
	
	def deploy_mysql_connectorj_package
	  super()
	  
	  connector = @config.getProperty(REPL_MYSQL_CONNECTOR_PATH)
		if connector != nil
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-sqlrouter/lib-ext/")
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-connector/lib/")
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-manager/lib/")
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/bristlecone/lib-ext/")
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-cluster-manager/lib/")
  		FileUtils.cp(connector, "#{get_deployment_basedir()}/tungsten-monitor/lib/")
  	end
	end
end