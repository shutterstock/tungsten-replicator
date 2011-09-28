require 'socket'

class DataserversChecks < GroupValidationCheck
  def initialize
    super(DATASOURCES, "datasource", "datasources")
    
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
  include ClusterHostCheck
  
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
  def enabled?
    if (v = @config.getNestedProperty([DEPLOYMENT_SERVICE])) != nil
      if v != get_member()
        # This replication service is not being deployed, so we don't need to check it
        return false
      end
    end
    
    super()
  end
  
  def get_applier_datasource
    ds = @config.getProperty(get_member_key(REPL_DATASOURCE))
    if ds.to_s() == ""
      raise "No applier datasource specified"
    end
    
    ConfigureDatabasePlatform.build(
      @config.getProperty([DATASOURCES, ds, REPL_DBTYPE]),
      @config.getProperty([DATASOURCES, ds, REPL_DBHOST]),
      @config.getProperty([DATASOURCES, ds, REPL_DBPORT]),
      @config.getProperty([DATASOURCES, ds, REPL_DBLOGIN]),
      @config.getProperty([DATASOURCES, ds, REPL_DBPASSWORD]), @config)
  end
  
  def get_extractor_datasource
    if @config.getProperty(get_member_key(REPL_ROLE)) == REPL_ROLE_DI
      ds = @config.getProperty(get_member_key(REPL_MASTER_DATASOURCE))
      if ds.to_s() == ""
        raise "No extractor datasource specified"
      end

      ConfigureDatabasePlatform.build(
        @config.getProperty([DATASOURCES, ds, REPL_DBTYPE]),
        @config.getProperty([DATASOURCES, ds, REPL_DBHOST]),
        @config.getProperty([DATASOURCES, ds, REPL_DBPORT]),
        @config.getProperty([DATASOURCES, ds, REPL_DBLOGIN]),
        @config.getProperty([DATASOURCES, ds, REPL_DBPASSWORD]), @config)
    else
      get_applier_datasource()
    end
  end
  
  def get_applier_key(key)
    [DATASOURCES, @config.getProperty(get_member_key(REPL_DATASOURCE)), key]
  end
  
  def get_extractor_key(key)
    [DATASOURCES, @config.getProperty(get_member_key(REPL_MASTER_DATASOURCE)), key]
  end
  
  def self.included(subclass)
    @submodules ||= []
    @submodules << subclass
  end

  def self.submodules
    @submodules || []
  end
end

module CreateServiceCheck
  def enabled?
    if [ConfigureServicePackage::SERVICE_UPDATE, ConfigureServicePackage::SERVICE_DELETE].include?(@config.getProperty(DEPLOYMENT_TYPE))
      false
    else
      super()
    end
  end
end

module ModifyServiceCheck
  def enabled?
    if [ConfigureServicePackage::SERVICE_UPDATE, ConfigureServicePackage::SERVICE_DELETE].include?(@config.getProperty(DEPLOYMENT_TYPE))
      super()
    else
      false
    end
  end
end

class BackupScriptAvailableCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  
  def set_vars
    @title = "Backup script availability check"
  end
  
  def validate    
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
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_BACKUP_METHOD)) == "script"
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
    
    begin
      thl_schema = "tungsten_#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}"
      get_applier_datasource.check_thl_schema(thl_schema)
    rescue => e
      warning(e.message)
    end
  end
end

class ServiceTransferredLogStorageCheck < ConfigureValidationCheck
  include ReplicationServiceValidationCheck
  include CreateServiceCheck
  
  def set_vars
    @title = "Service transferred log storage check"
  end
  
  def validate
    unless File.exists?(@config.getProperty(get_member_key(REPL_SVC_CONFIG_FILE)))
      if File.exists?(@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR)))
        dir_file_count = cmd_result("ls #{@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR))} | wc -l")
        if dir_file_count.to_i() > 0
          error("Transferred log directory #{@config.getProperty(get_member_key(REPL_RELAY_LOG_DIR))} already contains log files")
        end
      end
    end
  end
  
  def enabled?
    super() && @config.getProperty(get_member_key(REPL_RELAY_LOG_DIR)).to_s != ""
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
    if (extractor = get_extractor_datasource())
      if extractor == get_applier_datasource()
        error("Service '#{@config.getProperty(get_member_key(DEPLOYMENT_SERVICE))}' uses the same datasource for extracting and applying events")
      end
    end
  end
end