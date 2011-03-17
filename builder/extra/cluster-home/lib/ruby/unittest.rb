#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009-2010 Continuent, Inc.
# All rights reserved
#

# Unit tests for configure program. 

require 'tungsten/properties' 
require 'tungsten/configurator' 
require 'test/unit'

class TestConfigurator < Test::Unit::TestCase
  # Confirm that properties files can be written and loaded. 
  def test_properties
    config = Properties.new
    config.props['foo'] = "bar"
    config.store "/tmp/test.properties"
    config2 = Properties.new
    config2.load "/tmp/test.properties"
    assert_equal config.props, config2.props
    File.delete "/tmp/test.properties"
  end

  # Confirm integer property validation. 
  def test_integervalidation
    pd = PropertyDescriptor.new("Integer type", PV_INTEGER, "int", 0)
    pd.accept "13"
    assert_equal "13", pd.value

    # Should raise exception on bad value
    begin
      pd.accept "not an integer"
      raise "Allowed an invalid integer!"
    rescue PropertyValidatorException
      puts "OK"
    end
  end

  def test_stringvalidation
    pd = PropertyDescriptor.new("String type", PV_ANY, "string", "")
    pd.accept "fooo"
    assert_equal "fooo", pd.value
  end

  def test_booleanvalidation
    pd = PropertyDescriptor.new("Boolean type", PV_BOOLEAN, "boolean", "false")
    pd.accept "true"
    assert_equal "true", pd.value
    pd.accept "false"
    assert_equal "false", pd.value

    # Should raise exception on bad value
    begin
      pd.accept "bad to the bone"
      raise "Allowed an invalid boolean!"
    rescue PropertyValidatorException
      puts "OK"
    end
  end

  def test_filevalidation
    pf1 = PropertyDescriptor.new("File type", PV_READABLE_FILE, "file", "")
    pf1.accept "unittest.rb"
    assert_equal "unittest.rb", pf1.value

    # Should raise exception on non-existent file
    begin
      pf1.accept "does_not_exist"
      raise "Allowed an invalid file!"
    rescue PropertyValidatorException
      puts "OK"
    end

    pf2 = PropertyDescriptor.new("File type", PV_READABLE_DIR, "file", "")
    pf2.accept Dir.pwd
    assert_equal Dir.pwd, pf2.value

    # Should raise exception on non-existent file
    begin
      pf2.accept "does_not_exist"
      raise "Allowed an invalid directory!"
    rescue PropertyValidatorException
      puts "OK"
    end
  end

  def test_dbmsvalidation
    pd = PropertyDescriptor.new("DBMS type", PV_DBMSTYPE, "dbms", "dbms")
    pd.accept "mysql"
    assert_equal "mysql", pd.value
    pd.accept "oracle"
    assert_equal "oracle", pd.value

    # Should raise exception on bad value
    begin
      pd.accept "invalid dbms"
      raise "Allowed an invalid dbms!"
    rescue PropertyValidatorException
      puts "OK"
    end
  end

  def test_javaheapvalidation
    pd = PropertyDescriptor.new("Java heap", PV_JAVA_MEM_SIZE, "memsize", "256")
    pd.accept "128"
    assert_equal "128", pd.value
    pd.accept "1024"
    assert_equal "1024", pd.value
    pd.accept "2048"
    assert_equal "2048", pd.value

    # Should raise exception on low/high bound. 
    begin
      pd.accept "127"
      raise "Allowed value past low bound"
    rescue PropertyValidatorException
      puts "OK"
    end
    begin
      pd.accept "2049"
      raise "Allowed value past high bound"
    rescue PropertyValidatorException
      puts "OK"
    end
  end
end
