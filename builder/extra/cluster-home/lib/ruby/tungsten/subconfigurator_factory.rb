# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

require 'tungsten/subconfigurator_mysql'
require 'tungsten/subconfigurator_postgresql'
require 'tungsten/subconfigurator_oracle'

# Factory class to select appropriate sub-configurators. 
class SubConfiguratorFactory
  include ParameterNames

  # Initialize with base configurator.   
  def initialize(configurator)
    @configurator = configurator
  end

  # Select replicator sub-configurator by database.
  def repl_configurator()
    dbms = @configurator.config.props[GLOBAL_DBMS_TYPE]
    case dbms
      when "mysql" then SubConfiguratorMySQL.new(@configurator)
      when "oracle" then SubConfiguratorOracle.new(@configurator)
      when "postgresql" then SubConfiguratorPostgreSQL.new(@configurator)
    else 
      # Need to handle this as an error!
      raise SystemError, "Unknown database type: " + dbms
    end
  end
end