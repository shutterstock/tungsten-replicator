#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Simple utility class to transform files using regular expressions
require 'system_require'
system_require 'date'

class Transformer
  @@global_replacements = {}
  
  def self.add_global_replacement(key, value)
    if value == nil
      raise("Unable to make a global replacement using a nil value")
    end
    
    @@global_replacements[key] = value
  end
  
  def self.get_global_replacements
    @@global_replacements
  end
  
  # Initialize with the name of the to -> from files. 
  def initialize(infile, outfile = nil, end_comment = "")
    @infile = infile
    @outfile = outfile
    @end_comment = end_comment
    
    if defined?(Configurator)
      Configurator.instance.info("INPUT FROM: " + @infile)
    else
      puts "INPUT FROM: " + @infile
    end
    
    @output = []
    File.open(@infile) do |file|
      while line = file.gets
        @output << line
      end
    end
  end

  # Transform file by passing each line through a block that either
  # changes it or returns the line unchanged. 
  def transform(&block)
    transform_lines{
      |line|
      block.call(line)
    }
    output
  end
  
  def output
    if @outfile
      out = File.open(@outfile, "w")
      @output.each { |line| out.puts line }
      out.puts
    
      if @end_comment
        out.puts @end_comment + "AUTO-GENERATED: #{DateTime.now}"
      end
      out.close
      if defined?(Configurator)
        Configurator.instance.info("OUTPUT TO: " + @outfile)
      else
        puts "OUTPUT TO: " + @outfile
      end
    else
      return self.to_s
    end
  end
  
  def transform_lines(&block)
    @output.map!{
      |line|
      line_keys = line.scan(/[#]?([a-zA-Z0-9\._-]+)=.*/)
      if line_keys.length() > 0 && @@global_replacements.has_key?(line_keys[0][0])
        "#{line_keys[0][0]}=#{@@global_replacements[line_keys[0][0]]}\n"
      else
        block.call(line)
      end        
    }
  end
  
  def transform_values(method)
    @output.map!{
      |line|
      line_keys = line.scan(/[#]?([a-zA-Z0-9\._-]+)=.*/)
      if line_keys.length() > 0 && @@global_replacements.has_key?(line_keys[0][0])
        "#{line_keys[0][0]}=#{@@global_replacements[line_keys[0][0]]}\n"
      else
        line.gsub(/@\{[A-Za-z\._]+\}/){
          |match|
          r = method.call(match.tr("\@{}", "").split("."))
          
          if r.is_a?(Array)
            r.join(',')
          else
            r
          end
        }
      end
    }
  end
  
  def to_s
    return to_a.join('')
  end
  
  def to_a
    return @output
  end
end
