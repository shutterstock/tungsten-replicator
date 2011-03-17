module ConfigureDeploymentStepEnterpriseReplicator
  def deploy_replicator
    super()
    
    unless is_replicator?()
      return
    end
  end
end
