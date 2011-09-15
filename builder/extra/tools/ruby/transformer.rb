#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Simple utility class to transform files using regular expressions
require 'system_require'
system_require 'date'

class Transformer
  def add_fixed_replacement(key, value)
    if value == nil
      raise("Unable to add a fixed replacement using a nil value")
    end
    
    @fixed_replacements[key] = value
  end
  
  def add_fixed_addition(key, value)
    if value == nil
      raise("Unable to add a fixed addition using a nil value")
    end
    
    @fixed_additions[key] = value
  end
  
  def add_fixed_match(key, value)
    if value == nil
      raise("Unable to add a fixed match using a nil value")
    end
    
    match = value.split("/")
    match.shift
    
    if match.size != 2
      raise("Unable to add a fixed match using '#{value}'.  Matches must be in the form of /search/replacement/.")
    end
    
    @fixed_matches[key] = match
  end
  
  def set_fixed_properties(fixed_properties = [])
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    fixed_properties.each{
      |val|
      
      val_parts = val.split("=")
      last_char=val_parts[0][-1,1]
      if last_char == "+"
        add_fixed_addition(val_parts[0][0..-2], val_parts[1])
      elsif last_char == "~"
        add_fixed_match(val_parts[0][0..-2], val_parts[1])
      else
        add_fixed_replacement(val_parts[0], val_parts[1])
      end
    }
  end
  
  # Initialize with the name of the to -> from files. 
  def initialize(infile, outfile = nil, end_comment = "")
    @infile = infile
    @outfile = outfile
    @end_comment = end_comment
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    if defined?(Configurator)
      Configurator.instance.info("INPUT FROM: " + @infile)
    else
      puts "INPUT FROM: " + @infile
    end
    
    @output = []
    File.open(@infile) do |file|
      while line = file.gets
        @output << line.chomp()
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
      if line_keys.length() > 0 && @fixed_replacements.has_key?(line_keys[0][0])
        "#{line_keys[0][0]}=#{@fixed_replacements[line_keys[0][0]]}"
      else
        block.call(line)
      end        
    }
  end
  
  def transform_values(method)
    @output.map!{
      |line|
      line_keys = line.scan(/[#]?([a-zA-Z0-9\._-]+)=.*/)
      if line_keys.length() > 0 && @fixed_replacements.has_key?(line_keys[0][0])
        if line =~ /@\{[A-Za-z\._]+\}/
          if defined?(Configurator)
            Configurator.instance.debug("Fixed property value for '#{line_keys[0][0]}' is overriding a template value")
          end
        end
        
        "#{line_keys[0][0]}=#{@fixed_replacements[line_keys[0][0]]}"
      else
        line.gsub!(/@\{[A-Za-z\._]+\}/){
          |match|
          r = method.call(match.tr("\@{}", "").split("."))
          
          if r.is_a?(Array)
            r.join(',')
          else
            r
          end
        }
        
        line_keys = line.scan(/^[#]?([a-zA-Z0-9\._-]+)=(.*)/)
        if line_keys.size > 0
          if @fixed_additions.has_key?(line_keys[0][0])
            line_keys[0][1] += @fixed_additions[line_keys[0][0]]
            line = line_keys[0][0] + "=" + line_keys[0][1]
          end
        
          if @fixed_matches.has_key?(line_keys[0][0])
            line_keys[0][1].sub!(Regexp.new(@fixed_matches[line_keys[0][0]][0]), @fixed_matches[line_keys[0][0]][1])
            line = line_keys[0][0] + "=" + line_keys[0][1]
          end
        end
        
        line
      end
    }
  end
  
  def to_s
    return to_a.join("\n")
  end
  
  def to_a
    return @output
  end
end
