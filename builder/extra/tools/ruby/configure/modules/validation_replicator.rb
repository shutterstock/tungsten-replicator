require 'socket'

class DataserversChecks < GroupValidationCheck
  def initialize
    super(DATASERVERS, "dataserver", "dataservers")
    
    DataserverValidationCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Dataserver checks"
  end
end

module DataserverValidationCheck
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

class ReplicationServiceChecks < GroupValidationCheck
  def initialize
    super(REPL_SERVICES, "replication service", "replication services")
    
    ReplicationServiceValidationCheck.submodules().each{
      |klass|
      
      self.add_check(klass.new())
    }
  end
  
  def set_vars
    @title = "Replication service checks"
  end
end

module ReplicationServiceValidationCheck
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

class BackupMethodAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Backup method availability check"
  end
  
  def validate
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    case @config.getProperty([DATASERVERS, applier, REPL_BACKUP_METHOD])
    when "mysqldump"
      path = cmd_result("which mysqldump")
      info("mysqldump found at #{path}")
    when "xtrabackup"
      begin
        path = cmd_result("which innobackupex-1.5.1")
        info("xtrabackup found at #{path}")
      rescue
        error("Unable to find the innobackupex-1.5.1 script for backup")
      end
    when "pg_dump"
      path = cmd_result("which pg_dump")
      info("pg_dump found at #{path}")
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
  
  def enabled?
    applier = @config.getProperty(get_member_key(REPL_DATASERVER))
    if @config.getProperty([DATASERVERS, applier, DBMS_TYPE]) == "mysql"
      true
    else
      false
    end
  end
end

class THLStorageCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "THL storage check"
  end
  
  def validate
    repl_log_dir = @config.getProperty(get_member_key(REPL_LOG_DIR))
    if repl_log_dir
      if File.exists?(repl_log_dir) && !File.directory?(repl_log_dir)
        error("Replication log directory #{repl_log_dir} already exists as a file")
      else
        unless File.exists?(@config.getProperty(get_member_key(REPL_SVC_CONFIG_FILE)))
          if File.exists?(repl_log_dir)
            dir_file_count = cmd_result("ls #{repl_log_dir} | wc -l")
            if dir_file_count.to_i() > 0
              error("Replication log directory #{repl_log_dir} already contains log files but the service properties file is missing")
            end
          end
        end
      end
    end
    
    datasource_alias = @config.getProperty(get_member_key(REPL_DATASERVER))
    thl_schema = "tungsten_#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}"
    case @config.getProperty([DATASERVERS, datasource_alias, DBMS_TYPE])
    when "mysql"
      schemas = mysql_on("SHOW SCHEMAS LIKE '#{thl_schema}'", datasource_alias)
      if schemas != ""
        error("THL schema #{thl_schema} already exists at #{get_connection_summary_for(datasource_alias)}")
      end
    when "postgresql"
      warn("Currently unable to check for the THL schema in PostgreSQL")
    else
      error("An invalid database type (#{@config.getProperty([DATASERVERS, datasource_alias, DBMS_TYPE])}) is specified for replication service: #{get_member()}")
    end
  end
end

class TransferredLogStorageCheck < ConfigureValidationCheck
  def set_vars
    @title = "Transferred log storage check"
  end
  
  def validate
    # TODO
    
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
    # TODO - Update this check
    config_services = []
    
    current_services = []
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/tungsten-replicator/conf/static-*.properties'].each do |file| 
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

class ServiceNameCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
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
    Dir[@config.getProperty(CURRENT_RELEASE_DIRECTORY) + '/tungsten-replicator/conf/static-*.properties'].each do |file| 
      service_name = cmd_result("grep ^service.name= #{file} | awk -F = '{print $2}'")
      if service_name != ""
        current_services << service_name
      end
    end
    
    current_services
  end
end

class DifferentMasterSlaveCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
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