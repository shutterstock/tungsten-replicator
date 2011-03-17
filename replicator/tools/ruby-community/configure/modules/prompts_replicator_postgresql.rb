class PostgresConfigurePrompt < ConfigurePrompt
  def enabled?
    @config.getProperty(GLOBAL_DBMS_TYPE) == "postgresql"
  end
end

class PostgresRootDirectory < PostgresConfigurePrompt
  def initialize
    super(REPL_PG_ROOT, "Root directory for postgresql installation", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(GLOBAL_HOME_DIRECTORY)
  end
end

class PostgresDataDirectory < PostgresConfigurePrompt
  def initialize
    super(REPL_PG_HOME, "PostgreSQL data directory", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(REPL_PG_ROOT) + "/data"
  end
end

class PostgresArchiveDirectory < PostgresConfigurePrompt
  def initialize
    super(REPL_PG_ARCHIVE, "PostgreSQL archive location", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(REPL_PG_ROOT) + "/archive"
  end
end

class PostgresConfFile < PostgresConfigurePrompt
  def initialize
    super(REPL_PG_POSTGRESQL_CONF, "Location of postgresql.conf", PV_FILENAME)
  end
  
  def get_default_value
    @config.getProperty(REPL_PG_ROOT) + "/data/postgresql.conf"
  end
end

class PostgresArchiveTimeout < PostgresConfigurePrompt
  def initialize
    super(REPL_PG_ARCHIVE_TIMEOUT, "Timeout for sending unfilled WAL buffers (data loss window)", 
      PV_INTEGER, 60)
  end
end