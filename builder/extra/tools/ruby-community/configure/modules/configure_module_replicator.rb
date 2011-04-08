class ReplicatorConfigureModule < ConfigureModule
  def register_prompts(prompt_handler)
    prompt_handler.register_prompts([
      ReplicationServices.new(),
      ReplicationServiceMasters.new(),
      ReplicationServiceHosts.new(),
      ReplicationServiceRemoteHosts.new(),
      ConfigurePrompt.new(REPL_AUTOENABLE, "Auto-enable replicator after start-up", 
        PV_BOOLEAN, "true"),
      DatabaseInitScript.new(),
      DatabasePort.new(),
      ConfigurePrompt.new(REPL_DBLOGIN, "Database login for Tungsten", 
        PV_IDENTIFIER, "tungsten"),
      ConfigurePrompt.new(REPL_DBPASSWORD, "Database password", 
        PV_ANY, "secret"),
      THLStorageType.new(),
      THLStorageDirectory.new(),
      
      ReplicationShardIDMode.new(),
      ReplicationServiceShardIDMode.new(),
      ReplicationAllowUnsafeSQL.new(),
      ReplicationServiceAllowUnsafeSQL.new(),
      ReplicationAllowAllSQL.new(),
      ReplicationServiceAllowAllSQL.new(),
      
      MySQLBinlogDirectory.new(),
      MySQLBinlogPattern.new(),
      #MySQLConfigurePrompt.new(REPL_MYSQL_RO_SLAVE, "Make MySQL server read-only when acting as slave",
      #  PV_BOOLEAN, "true"),
      MySQLConfigurePrompt.new(REPL_USE_BYTES, "Transfer string data using binary format", 
        PV_BOOLEAN, "true"),
      MySQLConfigurePrompt.new(REPL_USE_DRIZZLE, "Use the Drizzle MySQL driver", 
        PV_BOOLEAN, "true"),
      MySQLConfigurePrompt.new(REPL_EXTRACTOR_USE_RELAY_LOGS, "Configure the extractor to access the binlog via local relay-logs?",
        PV_BOOLEAN, "false"),
      MySQLRelayLogDirectory.new(),
      MySQLAdvancedPrompt.new(REPL_THL_DO_CHECKSUM, "Execute checksum operations on THL log files", PV_BOOLEAN, "false"),
      MySQLAdvancedPrompt.new(REPL_THL_LOG_CONNECTION_TIMEOUT, "Number of seconds to wait for a connection to the THL log", PV_INTEGER, 600),
      MySQLAdvancedPrompt.new(REPL_THL_LOG_RETENTION, "How long do you want to keep THL files?", PV_ANY, "7d"),
      MySQLAdvancedPrompt.new(REPL_CONSISTENCY_POLICY, "Should the replicator stop or warn if a consistency check fails?", PV_ANY, "stop"),
      MySQLAdvancedPrompt.new(REPL_THL_LOG_FILE_SIZE, "File size in bytes for THL disk logs", PV_INTEGER, 1000000000),
      MySQLAdvancedPrompt.new(REPL_SVC_CHANNELS, "Number of channels to use for replication",
        PV_INTEGER, 1),
      ReplicationServiceChannels.new(),
      MySQLAdvancedPrompt.new(REPL_SVC_THL_PORT, "Port to use for THL operations", 
        PV_INTEGER, 2112),
      MySQLAdvancedPrompt.new(REPL_SVC_BINLOG_MODE, "Method to use for reading the binary log",
        PV_IDENTIFIER, "master"), 
        
        
      PostgresConfigurePrompt.new(REPL_PG_STREAMING, "Use streaming replication (available from PostgreSQL 9)",
        PV_BOOLEAN, "false"),
      PostgresRootDirectory.new(),
      PostgresDataDirectory.new(),
      PostgresArchiveDirectory.new(),
      PostgresConfFile.new(),
      PostgresArchiveTimeout.new(),
        
      #ConfigurePrompt.new(REPL_MASTER_VIP, "Master data source Virtual IP address (\"none\" for no Virtual IP)", 
      #  PV_ANY, "none"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_DEVICE, "Master data source Virtual IP device", 
      #  PV_ANY, "eth0:0"),
      #VIPConfigurePrompt.new(REPL_MASTER_VIP_IFCONFIG, "Full path to ifconfig", 
      #  PV_ANY, "/sbin/ifconfig"),
      
      BackupMethod.new(),
      BackupStorageDirectory.new(),
      BackupConfigurePrompt.new(REPL_BACKUP_DUMP_DIR, "Backup temporary dump directory",
        PV_FILENAME, "/tmp"),
      BackupConfigurePrompt.new(REPL_BACKUP_RETENTION, "Number of backups to retain", 
        PV_INTEGER, 3),
      BackupScriptPathConfigurePrompt.new(),
      BackupScriptCommandPrefixConfigurePrompt.new(),
      BackupScriptOnlineConfigurePrompt.new(),
      ConstantValuePrompt.new(REPL_MONITOR_INTERVAL, "Replication monitor interval", 
        PV_INTEGER, 3000),
      
      AdvancedPrompt.new(REPL_BUFFER_SIZE, "Replicator block commit size (min 1, max 100)",
        PV_REPL_BUFFER_SIZE, 10),
      AdvancedPrompt.new(REPL_JAVA_MEM_SIZE, "Replicator Java heap memory size in Mb (min 128)",
        PV_JAVA_MEM_SIZE, 512)
    ])
  end
  
  def register_validation_checks(validation_handler)
    validation_handler.register_checks([
      THLStorageCheck.new(),
      MySQLClientCheck.new(),
      MySQLLoginCheck.new(),
      MySQLPermissionsCheck.new(),
      MySQLReadableLogsCheck.new(),
      MySQLSettingsCheck.new(),
      ConnectorUserMySQLCheck.new(),
      PostgreSQLClientCheck.new(),
      PostgreSQLLoginCheck.new(),
      PostgreSQLPermissionsCheck.new(),
      PostgreSQLStandbyCheck.new(),
      PostgreSQLSettingsCheck.new(),
      ConnectorUserPostgreSQLCheck.new(),
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

require 'socket'
class VIPInterfaceAvailableCheck < ConfigureValidationCheck
  def set_vars
    @title = "VIP interface availability check"
  end
  
  def validate
    iface = @config.getProperty(REPL_MASTER_VIP_DEVICE)
    
    begin
      sock = UDPSocket.new()
   		buf = [iface,""].pack('a16h16')
  		sock.ioctl(0x8915, buf);
  		sock.close
  		iface_addr = buf[20..24].unpack("CCCC").join(".")
  		
  		if iface_addr == @config.getProperty(REPL_MASTER_VIP)
  		  info("#{iface} is already assigned as the VIP address on this host")
  		  
  		  if @config.getProperty(GLOBAL_HOST) != @config.getProperty(REPL_MASTERHOST)
  		    error("The VIP address is assigned to this host that is not the master")
  		  end
  		else
  		  error("#{iface} is in use with a different IP address on this host")
  		end
  	rescue
  	  info("#{iface} is usable on this host")
  	end
  end
  
  def enabled?
    (@config.getProperty(REPL_MASTER_VIP_DEVICE) != nil)
  end
end

class BackupMethodAvailableCheck < ConfigureValidationCheck
  def set_vars
    @title = "Backup method availability check"
  end
  
  def validate
    case @config.getProperty(REPL_BACKUP_METHOD)
    when "mysqldump"
      path = cmd_result("which mysqldump")
      info("mysqldump found at #{path}")
    when "pg_dump"
      path = cmd_result("which pg_dump")
      info("pg_dump found at #{path}")
    when "lvm"
    when "script"
      if File.executable(@config.getProperty(REPL_BACKUP_SCRIPT))
        info("The backup script is executable")
      else
        if File.exists(@config.getProperty(REPL_BACKUP_SCRIPT))
          error("The backup script (#{config.getProperty(REPL_BACKUP_SCRIPT)}) is not executable")
        else
          error("The backup script (#{config.getProperty(REPL_BACKUP_SCRIPT)}) does not exist")
        end
      end
    end
  end
end

class THLStorageCheck < ConfigureValidationCheck
  def set_vars
    @title = "THL storage check"
  end
  
  def validate
    ClusterConfigureModule.each_service(@config) {
      |parent_name,service_name,service_properties|
      
      unless File.exists?(service_properties[REPL_SVC_CONFIG_FILE])
        if File.exists?(service_properties[REPL_LOG_DIR])
          dir_file_count = cmd_result("ls #{service_properties[REPL_LOG_DIR]} | wc -l")
          if dir_file_count.to_i() > 0
            error("Replication log directory #{service_properties[REPL_LOG_DIR]} already contains log files")
          end
        end
      end  
    }
  end
end