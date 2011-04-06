# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#
# == Usage 
#   cd cluster-home/lib/ruby
#   ./migrate-mysql-tungsten.rb [options]
#
# == Options
#   -m, --master        The hostname that is the master
#   -s, --slaves        Hostnames for the slaves in the cluster
#   -u, --user          MySQL user
#   -p, --password      MySQL password
#   -P, --port          MySQL port
#

require 'service_migrator.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Configuration interrupted")
  exit 1
}

# Create and run the configurator. 
mig = ServiceMigrator.new(ARGV, STDIN)
mig.mysql_to_tungsten()