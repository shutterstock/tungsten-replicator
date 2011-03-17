
#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# == Synopsis 
#   Automatic preparation script for Tungsten.  This script verifies that
#   prerequisites are met for Tungsten installation. 
#
# == Examples
#   Here is how to invoke it using the corresponding wrapper script. 
#     prepare
#
# == Usage 
#   prepare [options]
#
# == Options
#   -h, --help          Displays help message
#   -V, --verbose       Verbose output
#
# == Author
#   Robert Hodges
#
# == Copyright
#   Copyright (c) 2010 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'tungsten/preparer.rb'

# Trap control-C interrupt. 
trap("INT") {
  puts("")
  puts("Configuration interrupted")
  exit 1
}

# Create and run the configurator. 
preparer = Preparer.new(ARGV, STDIN)
preparer.run
