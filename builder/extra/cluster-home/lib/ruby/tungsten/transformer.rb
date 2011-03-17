#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Simple utility class to transform files using regular expressions
require 'tungsten/system_require'
system_require 'date'

class Transformer
  # Initialize with the name of the to -> from files. 
  def initialize(infile, outfile, end_comment)
    @infile = infile
    @outfile = outfile
    @end_comment = end_comment
  end

  # Transform file by passing each line through a block that either
  # changes it or returns the line unchanged. 
  def transform
    puts "<< INPUT FROM: " + @infile
    output = []
    File.open(@infile) do |file|
      while line = file.gets
        transformed_line = yield line
        output.insert -1, transformed_line
      end
    end
      
    out = File.open(@outfile, "w")
    output.each { |line| out.puts line }
    if @end_comment
      out.puts @end_comment + "AUTO-GENERATED: #{DateTime.now}"
    end
    out.close
    puts ">> OUTPUT TO: " + @outfile
  end
end
