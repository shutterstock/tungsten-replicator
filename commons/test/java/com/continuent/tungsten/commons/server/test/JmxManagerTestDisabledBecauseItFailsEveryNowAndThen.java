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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.commons.server.test;

import javax.management.remote.JMXConnector;

import junit.framework.TestCase;

import com.continuent.tungsten.commons.jmx.JmxManager;

/**
 * A simple unit test of the JMX manager class. This also demonstrates how to
 * set up JMX, register MBeans, etc.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class JmxManagerTestDisabledBecauseItFailsEveryNowAndThen extends TestCase
{
    private String host        = "localhost";
    private int    port        = 1199;
    private String serviceName = "testBasic";

    /**
     * Demonstrate how to start and stop a JMX manager. We do it several times
     * to provide that JMX clean-up works.
     *
     * @throws Exception
     */
    public void testStartStop() throws Exception
    {
        for (int i = 0; i < 3; i++)
        {
            System.out.println(String.format(
                    "Creating service, host=%s, port=%d, serviceName=%s", host,
                    port, serviceName));
            JmxManager jmx = new JmxManager(host, port, serviceName);
            System.out.println(String.format(
                    "Starting service, host=%s, port=%d, serviceName=%s", host,
                    port, serviceName));
            jmx.start();
            jmx.stop();
        }
    }

    /**
     * Demonstrate that we can connect to the JMX manager with a client and
     * invoke an operation on an MBean.
     */
    public void testConnection()
    {
        // Start JMX.
        System.out.println(String.format(
                "Creating service, host=%s, port=%d, serviceName=%s", host,
                port, serviceName));
        JmxManager jmx = new JmxManager(host, port, serviceName);
        jmx.start();

        // Create an mbean implementation.
        Trial mbean = new Trial();

        // Register a bean.
        JmxManager.registerMBean(mbean, Trial.class);

        System.out.println(String.format(
                "Connecting to service, host=%s, port=%d, serviceName=%s", host,
                port, serviceName));
        // Connect to the JMX server and get a stub for this MBean.
        JMXConnector connector = JmxManager.getRMIConnector(host, port,
                serviceName);
        assertNotNull("Checking that connector is not null", connector);

        System.out.println(String.format(
                "Getting the mbean for class=%s", Trial.class.getName()));
        TrialMBean mbeanProxy = (TrialMBean) JmxManager.getMBeanProxy(
                connector, Trial.class, true);
        assertNotNull("Checking that proxy is not null", mbeanProxy);

        // Set and check count values via the MBean.
        mbeanProxy.setBeanCounter(14);
        assertEquals("Bean count set to 14 via proxy", 14, mbean.beanCounter);
        mbeanProxy.setBeanCounter(22);
        assertEquals("Bean count set to 22 via proxy", 22, mbean.beanCounter);

        // Stop the server.
        jmx.stop();
    }
}
