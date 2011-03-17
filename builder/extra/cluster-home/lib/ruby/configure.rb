
#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# == Synopsis 
#   Automatic configuration script for Tungsten Enterprise.  Run this 
#   script after unpacking the Tungsten Enterprise release.  You can 
#   rerun configuration using the tungsten.cfg only using the -b option. 
#
# == Examples
#   A standard configuration that queries user options, stores them in 
#   tungsten.cfg, and runs configuration. 
#     configure
#
#   Batch execution that runs configuration from tungsten.cfg without input. 
#     configure -b
#
#   Batch execution that uses an alternative tungsten.cfg file. 
#     configure -b -c mytungsten.cfg
#
# == Usage 
#   configure [options]
#
# == Options
#   -h, --help          Displays help message
#   -b, --batch         Batch execution from existing config file
#   -c, --config        Sets name of config file (default: tungsten.cfg)
#   -V, --verbose       Verbose output
#
# == Author
#   Robert Hodges
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'tungsten/configurator.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Configuration interrupted")
  exit 1
}

# Create and run the configurator. 
cfg = Configurator.new(ARGV, STDIN)
cfg.run
