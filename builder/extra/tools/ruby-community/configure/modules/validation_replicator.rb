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
  		  
  		  if @config.getProperty(HOST) != @config.getProperty(REPL_MASTERHOST)
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
    if @config.getProperty(REPL_LOG_DIR)
      if File.exists?(@config.getProperty(REPL_LOG_DIR)) && !File.directory?(@config.getProperty(REPL_LOG_DIR))
        error("Replication log directory #{@config.getProperty(REPL_LOG_DIR)} already exists as a file")
      end
    end
    
    @config.getPropertyOr(REPL_SERVICES).each{
      |service_alias,service_properties|
      
      if service_properties[REPL_LOG_DIR]
        unless File.exists?(service_properties[REPL_SVC_CONFIG_FILE])
          if File.exists?(service_properties[REPL_LOG_DIR])
            dir_file_count = cmd_result("ls #{service_properties[REPL_LOG_DIR]} | wc -l")
            if dir_file_count.to_i() > 0
              error("Replication log directory #{service_properties[REPL_LOG_DIR]} already contains log files")
            end
          end
        end  
      end
    }
  end
end

class TransferredLogStorageCheck < ConfigureValidationCheck
  def set_vars
    @title = "Transferred log storage check"
  end
  
  def validate
    if @config.getProperty(REPL_RELAY_LOG_DIR)
      if File.exists?(@config.getProperty(REPL_RELAY_LOG_DIR)) && !File.directory?(@config.getProperty(REPL_RELAY_LOG_DIR))
        error("Transferred log directory #{@config.getProperty(REPL_RELAY_LOG_DIR)} already exists as a file")
      end
    end
    
    @config.getPropertyOr(REPL_SERVICES).each{
      |service_alias,service_properties|
      
      if service_properties[REPL_RELAY_LOG_DIR]
        unless File.exists?(service_properties[REPL_SVC_CONFIG_FILE])
          if File.exists?(service_properties[REPL_RELAY_LOG_DIR])
            dir_file_count = cmd_result("ls #{service_properties[REPL_RELAY_LOG_DIR]} | wc -l")
            if dir_file_count.to_i() > 0
              error("Transferred log directory #{service_properties[REPL_RELAY_LOG_DIR]} already contains log files")
            end
          end
        end  
      end
    }
  end
end

class NoHiddenServicesCheck < ConfigureValidationCheck
  def set_vars
    @title = "No hidden services check"
  end
  
  def validate
    config_services = ConfigurePackageCluster.services_list(@config)
    
    current_services = []
    Dir[@config.getProperty(BASEDIR) + '/tungsten-replicator/conf/static-*.properties'].each do |file| 
      service_name = cmd_result("grep ^service.name= #{file} | awk -F = '{print $2}'")
      if service_name != ""
        current_services << service_name
      end
    end
    
    debug("Current services: #{current_services.join(',')}")
    debug("Configured services: #{config_services.join(',')}")
    missing_services = current_services - config_services
    missing_services.each{
      |service_name|
      error("Missing configuration information for replication service '#{service_name}'")
    }
  end
end

class DataserversChecks < GroupValidationCheck
  def initialize
    super(DATASERVERS, "dataserver", "dataservers")
    
    add_checks(
      MySQLClientCheck.new(),
      MySQLLoginCheck.new(),
      MySQLPermissionsCheck.new(),
      MySQLReadableLogsCheck.new(),
      MySQLSettingsCheck.new(),
      ConnectorUserMySQLCheck.new(),
      PostgreSQLSystemUserCheck.new(),
      PostgreSQLClientCheck.new(),
      PostgreSQLLoginCheck.new(),
      PostgreSQLPermissionsCheck.new(),
      PostgreSQLStandbyCheck.new(),
      PostgreSQLSettingsCheck.new(),
      ConnectorUserPostgreSQLCheck.new()
    )
  end
  
  def set_vars
    @title = "Dataserver checks"
  end
end

class ReplicationServiceChecks < GroupValidationCheck
  def initialize
    super(REPL_SERVICES, "replication service", "replication services")
    
    add_checks(
      ServiceNameCheck.new(),
      DifferentMasterSlaveCheck.new()
    )
  end
  
  def set_vars
    @title = "Replication service checks"
  end
end

class ServiceNameCheck < ConfigureValidationCheck
  include GroupValidationCheckMember
  
  def set_vars
    @title = "Service name check"
  end
  
  def validate
    service_name = @config.getProperty(get_member_key(DEPLOYMENT_SERVICE))
    
    case @config.getProperty(DEPLOYMENT_TYPE)
    when ConfigureServicePackage::SERVICE_UPDATE, ConfigureServicePackage::SERVICE_DELETE
      unless current_services().include?(service_name)
        error("Service '#{service_name}' is not defined on this host")
      end
    when ConfigureServicePackage::SERVICE_CREATE
      if current_services().include?(service_name)
        error("Service '#{service_name}' is already defined on this host")
      end
    else
      # Do nothing here for now
    end
  end
  
  def current_services
    current_services = []
    Dir[@config.getProperty(BASEDIR) + '/tungsten-replicator/conf/static-*.properties'].each do |file| 
      service_name = cmd_result("grep ^service.name= #{file} | awk -F = '{print $2}'")
      if service_name != ""
        current_services << service_name
      end
    end
    
    current_services
  end
end

class DifferentMasterSlaveCheck < ConfigureValidationCheck
  include GroupValidationCheckMember
  
  def set_vars
    @title = "Different master/slave datasource check"
  end
  
  def validate
    if (extractor = @config.getProperty(get_member_key(REPL_EXTRACTOR_DATASERVER)))
      if extractor == @config.getProperty(get_member_key(REPL_DATASERVER))
        error("Service '#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}' uses the same datasource for extracting and applying events")
      end
    end
  end
end