/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.plugin;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


import junit.framework.Assert;
import junit.framework.TestCase;

public class TestPluginLoader extends TestCase
{

    static Logger       logger  = null;
    final static String dpName  = "com.continuent.tungsten.replicator.plugin.DummyPlugin";
    final static String dpiName = "com.continuent.tungsten.replicator.plugin.DummyPluginImplementation";

    public void setUp() throws Exception
    {
        if (logger == null)
        {
            logger = Logger.getLogger(TestPluginLoader.class);
            BasicConfigurator.configure();
        }
    }

    /*
     * Simple test that loads plugin and tests setter and getter methods.
     */
    public void testDummyPlugin() throws Exception
    {
        DummyPlugin dp = (DummyPlugin) PluginLoader.load(dpName);
        PluginConfigurator.setParameter(dp, "setA", "valueA");
        Assert.assertEquals("valueA", PluginConfigurator.getParameter(dp,
                "getA"));
        Assert.assertEquals(null, PluginConfigurator.getParameter(dp, "getB"));
        PluginConfigurator.setParameter(dp, "setB", "valueB");
        Assert.assertEquals("valueB", PluginConfigurator.getParameter(dp,
                "getB"));

    }

    /*
     * Test loading of plugin implementation defined by interface that extends
     * ReplicatorPlugin.
     */
    public void testDummyPluginInterface() throws Exception
    {
        DummyPluginInterface dpi = (DummyPluginInterface) PluginLoader
                .load(dpiName);
        PluginConfigurator.setParameter(dpi, "setC", "valueC");
        Assert.assertEquals("valueC", PluginConfigurator.getParameter(dpi,
                "getC"));
        dpi.configure(null);
        dpi.prepare(null);
        dpi.release(null);
    }

    /*
     * Check that usual error situations result an exception.
     */
    public void testErrors() throws Exception
    {

        /*
         * Trying to load plugin that does not exist must result an exception.
         */
        try
        {
            PluginLoader
                    .load("com.continuent.tungsten.replicator.plugin.PluginThatDoesNotExist");
            throw new Exception(
                    "Trying to load non-existing class does not result an exception");
        }
        catch (PluginException e)
        {

        }

        DummyPluginInterface dpi = (DummyPluginInterface) PluginLoader
                .load(dpiName);

        PluginConfigurator.setParameter(dpi, "setStringVal", "sval");
        Assert.assertEquals("sval", PluginConfigurator.getParameter(dpi,
                "getStringVal"));

        /*
         * setStringVal takes string as an argument, trying to set integer must
         * result an exception.
         */
        try
        {
            PluginConfigurator
                    .setParameter(dpi, "setStringVal", new Integer(1));
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {
            // e.printStackTrace();
        }

        PluginConfigurator.setParameter(dpi, "setIntVal", 1);
        Assert.assertEquals(1, PluginConfigurator
                .getParameter(dpi, "getIntVal"));
        /*
         * setIntVal takes Integer as an argument, trying to set double or long
         * must result an exception.
         */
        try
        {
            PluginConfigurator.setParameter(dpi, "setIntVal", 0.1D);
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {

        }

        try
        {
            PluginConfigurator.setParameter(dpi, "setIntVal", 1L);
            throw new Exception("Illegal argument exception not thrown");
        }
        catch (PluginException e)
        {

        }

        /*
         * Trying to call non-existent method must result an exception.
         */
        try
        {
            PluginConfigurator
                    .setParameter(dpi, "callNonExistentMethod", "foo");
            throw new Exception("Exception not thrown");
        }
        catch (PluginException e)
        {

        }

        /*
         * Trying to load plugin that does not implement desired interface must
         * result an exception.
         */
        try
        {
            DummyPluginInterface ii = (DummyPluginInterface) PluginLoader
                    .load(dpName);
            // Just some use of ii to avoid warning
            PluginConfigurator.getParameter(ii, "getStringVal");
        }
        catch (ClassCastException e)
        {
            // e.printStackTrace();
        }
    }
}
