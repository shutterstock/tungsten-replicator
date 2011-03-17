if defined?(DirectDeployment)
  DirectDeployment.module_eval(%q(
    def get_deployment_object_modules
      modules = [
        ConfigureDeploymentStepDirect,
        ]

      case @config.getProperty(GLOBAL_DBMS_TYPE)
      when "mysql"
        modules << ConfigureDeploymentStepMySQL
        modules << ConfigureDeploymentStepEnterpriseMySQL
        modules << ConfigureDeploymentStepReplicationDataservice
      when "postgresql"
        modules << ConfigureDeploymentStepPostgresql
        modules << ConfigureDeploymentStepEnterprisePostgresql
      else
        raise "Invalid value for #{GLOBAL_DBMS_TYPE}"
      end

#      modules << ConfigureDeploymentStepBristlecone
      modules << ConfigureDeploymentStepServices

      modules
    end
  ))
end