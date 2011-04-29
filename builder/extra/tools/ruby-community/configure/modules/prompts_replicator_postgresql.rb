class PostgresConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(DBMS_TYPE) == "postgresql"
  end
  
  def get_default_value
    begin
      get_pgsql_default_value()
    rescue => e
      @default
    end
  end
  
  def get_pgsql_default_value
    raise "Undefined function"
  end
  
  # Execute mysql command and return result to client. 
  def pgsql(command, hostname = nil)
    user = @config.getProperty(REPL_DBLOGIN)
    password = @config.getProperty(REPL_DBPASSWORD)
    port = @config.getProperty(REPL_DBPORT)
    if hostname == nil
      hosts = @config.getProperty(HOSTS).split(",")
      hostname = hosts[0]
    end

    ssh_result("echo '#{command}' | psql -q -A -t", true, hostname)
  end
end

class PostgresStreamingReplication < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_STREAMING, "Use streaming replication (available from PostgreSQL 9)",
      PV_BOOLEAN, "false")
  end
end

class PostgresRootDirectory < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_ROOT, "Root directory for postgresql installation", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(HOME_DIRECTORY)
    super()
  end
  
  def get_pgsql_default_value
    def_data_dir = pgsql("SHOW data_directory;")
    if def_data_dir.to_s() == "" || !def_data_dir.include?("/") || def_data_dir.include?("psql")
      raise "Cannot determine"
    else
      def_data_dir[0, def_data_dir.rindex('/')]
    end
  end
end

class PostgresDataDirectory < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_HOME, "PostgreSQL data directory", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(REPL_PG_ROOT) + "/data"
  end
  
  def get_default_value
    @default = @config.getProperty(HOME_DIRECTORY)
    super()
  end
  
  def get_pgsql_default_value
    def_data_dir = pgsql("SHOW data_directory;")
    if def_data_dir.to_s() == ""
      raise "Cannot determine"
    else
      def_data_dir
    end
  end
end

class PostgresArchiveDirectory < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_ARCHIVE, "PostgreSQL archive location", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(get_member_key(REPL_PG_ROOT)) + "/archive"
  end
end

class PostgresConfFile < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_POSTGRESQL_CONF, "Location of postgresql.conf", PV_FILENAME)
  end
  
  def get_default_value
    @default = @config.getProperty(get_member_key(REPL_PG_ROOT)) + "/data/postgresql.conf"
  end
  
  def get_pgsql_default_value
    def_config_path = pgsql("SHOW config_file;")
    if def_config_path.to_s() == ""
      raise "Cannot determine"
    else
      def_config_path
    end
  end
end

class PostgresArchiveTimeout < PostgresConfigurePrompt
  include GroupConfigurePromptMember
  
  def initialize
    super(REPL_PG_ARCHIVE_TIMEOUT, "Timeout for sending unfilled WAL buffers (data loss window)", 
      PV_INTEGER, 60)
  end
end