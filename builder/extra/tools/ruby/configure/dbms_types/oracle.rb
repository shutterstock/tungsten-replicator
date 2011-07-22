DBMS_ORACLE = "oracle"
DBMSTypePrompt.add_dbms_type(DBMS_ORACLE)

#
# Prompts
#

class OracleConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(get_member_key(DBMS_TYPE)) == DBMS_ORACLE
  end
  
  def get_default_value
    begin
      get_oracle_default_value()
    rescue => e
      @default
    end
  end
  
  def get_oracle_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def oracle(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    raise "Update this to build the proper command"
    ssh_result("echo '#{command}' | psql -q -A -t", true, hostname)
  end
end

class OracleService < OracleConfigurePrompt
  include DataserverPrompt
  
  def initialize
    super(REPL_ORACLE_SERVICE, "Oracle Service", 
      PV_IDENTIFIER)
  end
end

#
# Validation
#

class OracleValidationCheck < ConfigureValidationCheck
  def get_variable(name)
    oracle("show #{name}").chomp.strip;
  end
  
  def enabled?
    super() && @config.getProperty(DBMS_TYPE) == DBMS_ORACLE
  end
end

#
# Deployment
#

module ConfigureDeploymentStepOracle
  include DatabaseTypeDeploymentStep
  
  def transform_replication_dataservice_line(line, service_name, service_config)
		if line =~ /replicator.applier.oracle.service=/
		  "replicator.applier.oracle.service=#{service_config.getProperty(REPL_ORACLE_SERVICE)}"
		else
		  super(line, service_name, service_config)
		end
	end
	
	def get_replication_dataservice_template(service_config)
    if is_applier(service_config, DBMS_ORACLE)
      "#{get_deployment_basedir()}/tungsten-replicator/samples/conf/sample.static.properties.oracle"
  	else
  	  super(service_config)
  	end
	end
end