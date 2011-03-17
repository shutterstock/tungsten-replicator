module ConfigureDeploymentStepDirect
  def get_deployment_basedir
    Configurator.instance.get_base_path()
  end
end