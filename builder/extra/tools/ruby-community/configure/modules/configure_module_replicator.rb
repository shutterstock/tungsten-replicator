REPL_MODE_MS = "master-slave"
REPL_MODE_DI = "direct"

class ReplicatorConfigureModule < ConfigureModule
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      DataServers.new(),

      #MySQLReplicationServiceMode.new(),
      #ReplicationServiceMasters.new(),
      #ReplicationServiceHosts.new(),
      #ReplicationServiceRemoteHosts.new(),
      #MySQLDirectReplicationExtractHost.new(),
      #MySQLDirectReplicationExtractPort.new(),
      #MySQLDirectReplicationExtractLogin.new(),
      #MySQLDirectReplicationExtractPassword.new(),
      #ReplicationServiceChannels.new(),
      #ReplicationServiceAutoEnable.new(),
      #ReplicationServiceTHLPort.new(),
      #MySQLReplicationUseRelayLogs.new(),
      
      #ReplicationShardIDMode.new(),
      #ReplicationServiceShardIDMode.new(),
      #ReplicationAllowUnsafeSQL.new(),
      #ReplicationServiceAllowUnsafeSQL.new(),
      #ReplicationAllowAllSQL.new(),
      #ReplicationServiceAllowAllSQL.new(),
      
      #MySQLConfigurePrompt.new(REPL_MYSQL_RO_SLAVE, "Make MySQL server read-only when acting as slave",
      #  PV_BOOLEAN, "true"),
      #MySQLAdvancedPrompt.new(REPL_USE_BYTES, "Transfer string data using binary format", 
      #  PV_BOOLEAN, "true"),
      #MySQLConfigurePrompt.new(REPL_USE_DRIZZLE, "Use the Drizzle MySQL driver", 
      #  PV_BOOLEAN, "true"),
      
      #ConfigurePrompt.new(REPL_MASTER_VIP, "Master data source Virtual IP address (\"none\" for no Virtual IP)", 
      #  PV_ANY, "none"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_DEVICE, "Master data source Virtual IP device", 
      #  PV_ANY, "eth0:0"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_IFCONFIG, "Full path to ifconfig", 
      #  PV_ANY, "/sbin/ifconfig"),
      
      #BackupMethod.new(),
      #BackupStorageDirectory.new(),
      #BackupConfigurePrompt.new(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory",
      #  PV_FILENAME, "/tmp"),
      #BackupConfigurePrompt.new(REPL_BACKUP_RETENTION, "Number of backups to retain", 
      #  PV_INTEGER, 3),
      #BackupScriptPathConfigurePrompt.new(),
      #BackupScriptCommandPrefixConfigurePrompt.new(),
      #BackupScriptOnlineConfigurePrompt.new(),
      
      #AdvancedPrompt.new(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
      #  PV_REPL_BUFFER_SIZE, 10),
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
      THLStorageCheck.new(),
#      NoHiddenServicesCheck.new(),

      DataserversChecks.new(),
      VIPInterfaceAvailableCheck.new(),
      BackupMethodAvailableCheck.new(),
    ])
  end
  
  def include_module_for_package?(package)
    if package.is_a?(ConfigurePackageCluster)
      true
    else
      false
    end
  end
end