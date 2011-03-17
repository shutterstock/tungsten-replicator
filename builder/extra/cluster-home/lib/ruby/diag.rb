#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# == Synopsis 
#   Collects various debugging information from Tungsten components. Includes
#   configuration, logs, internal data structures, etc.  
#
# == Examples
#   Include last 10K of every log.  
#     diag --log-size 10240
#
# == Usage 
#   diag [options]
#
# == Options
#   -h, --help       Displays help message
#   -ls, --log-size  How much of a log to take in bytes. Default: 100K
#
# == Author
#   Linas Virbalas
#
# == Copyright
#   Copyright (c) 2009 Continuent, Inc.  All rights reserved.
#   http://www.continuent.com

require 'optparse'
require 'ftools'
require 'fileutils'
require 'zip/zip'
require 'find'

@LOG_SIZE = 100 * 1024
@MANAGER_PORT = 9997
@ROUTER_PORT = 10999

# See for more constants bellow.

# Usage description.
def output_help
  puts "Tungsten diagnostics gathering utility"
  puts "Syntax: diag [options]"
  puts "-h, --help            Displays this help message"
  puts "-l, --log-size N      Set maximum log file size"
  puts "-m, --manager-port N  RMI port of the manager (default: #@MANAGER_PORT)"
  puts "-s, --router-port N   RMI port of the sql-router (default: #@ROUTER_PORT)"
end

# Parse command line arguments. 
def parsed_options?
    
  opts=OptionParser.new
  opts.on("-h", "--help")                 {|val| output_help; exit 0}
  opts.on("-l", "--log-size N",  Integer) {|val| @LOG_SIZE = val}
  opts.on("-m", "--manager-port N",  Integer) {|val| @MANAGER_PORT = val}
  opts.on("-s", "--router-port N",  Integer) {|val| @ROUTER_PORT = val}
      
  opts.parse!(ARGV) rescue 
  begin 
    puts "Argument parsing failed" 
    return false
  end
    
  true      
end

# Copy specified log's last n bytes.
def copy_log(src_path, dest_path, bytes)
  if File.exist?(src_path)
    fout = File.open(dest_path, 'w')

    File.open(src_path, "r") do |f|
      if bytes < File.size(src_path)
        f.seek(-bytes, IO::SEEK_END)
      end
      while (line = f.gets)
        fout.puts line
      end
    end

    fout.close
  end
end

# Compress folder contents into a zip.
def zip_folder(src_path, zip_file)
  Zip::ZipFile.open(zip_file, Zip::ZipFile::CREATE) do |zipfile|
    Find.find(src_path) do |path|
      zipfile.add(path, path)
    end
    zipfile.close 
  end
end

# Main.
if parsed_options?

  # Constants.
  
  ROOT_DIR = "../../"
  OUT_NAME = "tungsten-diag-" + Time.now.localtime.strftime("%Y-%m-%d-%H-%M")
  OUT_DIR = OUT_NAME + "/"
  OUT_ZIP = OUT_NAME + ".zip"
  CCTRL = ROOT_DIR + "tungsten-manager/bin/cctrl"
  CCTRL_CMD = "ls -l\nphysical\nls -lR\nquit"
  CCTRL_MANAGER_DIAG = "physical\n*/*/manager/ServiceManager/diag\nquit"
  CCTRL_ROUTER_DIAG = "physical\n*/*/router/RouterManager/diag\nquit"
  THL = ROOT_DIR + "tungsten-replicator/bin/thl"
  TREPCTL = ROOT_DIR + "tungsten-replicator/bin/trepctl"

  # Create a temporary folder which will be archived afterwards.

  puts "Gathering diagnostic information:"
  
  Dir.mkdir(OUT_DIR) unless File.directory?(OUT_DIR)
  
  # Call "diag" commands.
  
  cctrl_out = OUT_DIR + "cctrl.out"
  
  puts "* Calling diag on replicator..."
  system TREPCTL + " diag > /dev/null"

  puts "* Calling diag on manager..."
  system "printf '" + CCTRL_MANAGER_DIAG + "' | " + CCTRL + " >> " + cctrl_out

  puts "* Calling diag on sql-router..."
  system "printf '" + CCTRL_ROUTER_DIAG + "' | " + CCTRL + " >> " + cctrl_out
  
  sleep 1 # Wait for logs to be written.

  # Gather logs.

  puts "* Logs (#@LOG_SIZE B for each)..."

  #File.copy(ROOT_DIR + "tungsten-replicator/log/trep.log", OUT_DIR + "trep.log")
  copy_log(ROOT_DIR + "tungsten-replicator/log/trepsvc.log", OUT_DIR + "trepsvc.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "tungsten-replicator/log/trep.log", OUT_DIR + "trep.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "tungsten-manager/log/tmsvc.log", OUT_DIR + "tmsvc.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "tungsten-monitor/log/monitor.log", OUT_DIR + "monitor.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "tungsten-sqlrouter/log/router.log", OUT_DIR + "router.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "tungsten-connector/log/connector.log", OUT_DIR + "connector.log", @LOG_SIZE)
  copy_log(ROOT_DIR + "gossiprouter/log/gossip.log", OUT_DIR + "gossip.log", @LOG_SIZE)
  
  # Gather datasources.

  puts "* Datasources:"
  
  Find.find(ROOT_DIR + "cluster-home/conf/cluster/") do |path|
    if FileTest.directory?(path) and File.basename(path) == "datasource"
      puts "    " + path
      FileUtils.cp_r(path, OUT_DIR + "datasource/")
    end
  end

  # Gather cctrl output.

  puts "* cctrl output..."

  cmd = "printf '" + CCTRL_CMD + "' | " + CCTRL + " >> " + cctrl_out
  system cmd
  
  # Gather thl tool output.

  puts "* thl tool output..."
  
  thl_out =  OUT_DIR + "thl.out"
  cmd = THL + " info > " + thl_out
  system cmd

  # Compress all into a zip.

  puts "Compressing..."

  zip_folder(OUT_DIR, OUT_ZIP)
  
  # Clean up.

  puts "Removing temporary folder..."

  FileUtils.rm_r(OUT_DIR)
  
  puts "DONE: " + OUT_ZIP
end