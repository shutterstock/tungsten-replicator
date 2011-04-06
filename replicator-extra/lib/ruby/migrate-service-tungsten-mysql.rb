# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2010 Continuent, Inc.
# All rights reserved
#

require 'service_migrator.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Migration interrupted")
  exit 1
}

# Create and run the configurator. 
mig = ServiceMigrator.new(ARGV, STDIN)
mig.tungsten_to_mysql()