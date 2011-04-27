class ReplicatorConfigureModule < ConfigureModule
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      #MySQLConfigurePrompt.new(REPL_MYSQL_RO_SLAVE, "Make MySQL server read-only when acting as slave",
      #  PV_BOOLEAN, "true"),
      #,
      
      #ConfigurePrompt.new(REPL_MASTER_VIP, "Master data source Virtual IP address (\"none\" for no Virtual IP)", 
      #  PV_ANY, "none"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_DEVICE, "Master data source Virtual IP device", 
      #  PV_ANY, "eth0:0"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_IFCONFIG, "Full path to ifconfig", 
      #  PV_ANY, "/sbin/ifconfig"),
    ])
  end
end