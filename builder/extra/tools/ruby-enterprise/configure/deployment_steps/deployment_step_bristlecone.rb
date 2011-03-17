module ConfigureDeploymentStepBristlecone
  def get_deployment_methods
    [
      ConfigureDeploymentMethod.new("deploy_bristlecone")
    ]
  end
  module_function :get_deployment_methods
  
  # Update configuration for bristlecone.
  def deploy_bristlecone
    Configurator.instance.write_header "Performing Bristlecone performance test configuration"
    
    write_sample_connector_xml()
    write_sample_readwrite()
    write_sample_readonly()
    
    write_evaluator_readonly()
    write_evaluator_readwrite()
  end
  
  def write_sample_connector_xml
    if (is_connector?() && File.exists?("#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.connector.xml"))
      transformer = Transformer.new(
        "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.connector.xml",
        "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.connector.xml", nil)

      transformer.transform { |line|
        if line =~ /user="(.*)"/ then
          line.sub $1, @config.getProperty(CONN_CLIENTLOGIN)
        elsif line =~ /password="(.*)"/ then
          line.sub $1, @config.getPropertyOr(CONN_CLIENTPASSWORD, "")
        elsif line =~ /url=".*(localhost).*"/ then
          line.sub $1, @config.getProperty(GLOBAL_HOST) + ":" + @config.getProperty(CONN_LISTEN_PORT)
        else
          line
        end
      }
    end
  end
  
  def write_sample_readwrite
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.readwrite.xml",
      "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.readwrite.xml", nil)

    transformer.transform { |line|
      if line =~ /user="(.*)"/ then
        line.sub $1, @config.getProperty(CONN_CLIENTLOGIN)
      elsif line =~ /password="(.*)"/ then
        line.sub $1, @config.getPropertyOr(CONN_CLIENTPASSWORD, "")
      elsif line =~ /url=".*(default).*"/ then
        line.sub $1, @config.getProperty(GLOBAL_DSNAME)
      else
        line
      end
    }
  end
  
  def write_sample_readonly
    transformer = Transformer.new(
      "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.readonly.xml",
      "#{get_deployment_basedir()}/bristlecone/config/evaluator/sample.readonly.xml", nil)

    transformer.transform { |line|
      if line =~ /user="(.*)"/ then
        line.sub $1, @config.getProperty(CONN_CLIENTLOGIN)
      elsif line =~ /password="(.*)"/ then
        line.sub $1, @config.getPropertyOr(CONN_CLIENTPASSWORD, "")
      elsif line =~ /url=".*(default).*"/ then
        line.sub $1, @config.getProperty(GLOBAL_DSNAME)
      else
        line
      end
    }
  end

  def write_evaluator_readonly
    # Create script to start bristlecone test with read-only connection.
    script = "#{get_deployment_basedir()}/cluster-home/bin/evaluator_readonly"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Run bristlecone evaluator with SQL Router readonly connection"
    out.puts "BRI_HOME=`dirname $0`/../../bristlecone"
    out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.readonly.xml $*"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end

  def write_evaluator_readwrite
    # Create script to start bristlecone test with read-write connection.
    script = "#{get_deployment_basedir()}/cluster-home/bin/evaluator_readwrite"
    out = File.open(script, "w")
    out.puts "#!/bin/bash"
    out.puts "# Run bristlecone evaluator with SQL Router read/write connection"
    out.puts "BRI_HOME=`dirname $0`/../../bristlecone"
    out.puts "$BRI_HOME/bin/evaluator_tungsten.sh  $BRI_HOME/config/evaluator/sample.readwrite.xml $*"
    out.puts "# AUTO-CONFIGURED: #{DateTime.now}"
    out.chmod(0755)
    out.close
    info "GENERATED FILE: " + script
  end
end