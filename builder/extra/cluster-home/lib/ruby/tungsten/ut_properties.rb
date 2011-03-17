#!/usr/bin/env ruby 
#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Unit tests for properties. 

require 'tungsten/properties' 
require 'test/unit'

class TestProperties < Test::Unit::TestCase
  # Verify property get/set. 
  def test_set_get
    p = Properties.new
    assert_nil(p.getProperty("foo"))

    p.setProperty("foo", "bar")
    assert_equal("bar", p.getProperty("foo"))

    p.setProperty("foo", "foo")
    assert_equal("foo", p.getProperty("foo"))
  end

  # Very setting of defaults. 
  def test_set_default
    p = Properties.new
    assert_nil(p.getProperty("foo"))

    p.setDefault("foo", "bar")
    assert_equal("bar", p.getProperty("foo"))

    p.setDefault("foo", "foo")
    assert_equal("bar", p.getProperty("foo"))
  end

  # Confirm that properties files can be written to file and reloaded. 
  def test_read_write
    p = Properties.new
    p.hash['foo'] = "bar"
    p.store "/tmp/test.properties"
    p2 = Properties.new
    p2.load "/tmp/test.properties"
    assert_equal(p.hash, p2.hash)
    File.delete "/tmp/test.properties"
  end

  # Confirm that we can load properties from a delimited string of 
  # key/value pairs. 
  def test_list_set
    p = Properties.new
    p.setPropertiesFromList("foo=a b;bar=b+2", ";");
    p2 = Properties.new
    p2.setProperty("foo", "a b");
    p2.setProperty("bar", "b+2");
    assert_equal(p.hash, p2.hash)
  end

  # Confirm that we get an exception if we save to an invalid/unwritable
  # file. 
  def test_bad_file
    p = Properties.new
    p.setProperty("foo", "bar")
    begin
      p.store("/nonexistent/file")
    rescue
      exception = "ok"
      #puts("Caught expected exception")
    end
    assert_equal("ok", exception);
  end
end
