
#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# == Synopsis 
#   Service configuration script for Tungsten Replicator.  Run this 
#   script after configuring the base installation using 'configure'.  
#
# == Examples
#   A simple command to create a master service.  You can start the 
#   service using 'trepctl -service local start' after running this 
#   command. 
#     configure-service --create -config=tungsten.cfg --role=master name
#
#   Another command to remove 
#     configure -b
#
#   Batch execution that uses an alternative tungsten.cfg file. 
#     configure -b -c mytungsten.cfg
#
# == Usage 
#   configure-service {-C | -D | -U} [options] name
#
# == Options
#   -h, --help          Displays help message and quits
#   -C, --create        Create a new service
#   -D, --delete        Delete a service and remove logs
#   -U, --update        Update the service definition
#
# == Author
#   Robert Hodges
#
# == Copyright
#   Copyright (c) 2011 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'tungsten/service_configurator.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Configuration interrupted")
  exit 1
}

# Create and run the configurator. 
cfg = ServiceConfigurator.new(ARGV, STDIN)
cfg.run
