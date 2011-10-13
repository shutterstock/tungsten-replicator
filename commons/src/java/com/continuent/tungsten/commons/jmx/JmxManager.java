/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Contributor(s): Edward Archibald, Stephane Giron
 */

package com.continuent.tungsten.commons.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.apache.log4j.Logger;

/**
 * Encapsulates JMX server start/stop and provides static utility methods to
 * register MBeans on the server side as well as get proxies for them on the
 * client side.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class JmxManager implements NotificationListener
{
    private static final Logger  logger                = Logger.getLogger(JmxManager.class);

    // RMI registry and connector server we are managing.
    protected Registry           rmiRegistry;
    protected JMXConnectorServer jmxConnectorServer;
    // TODO: Do not destroy registry if we have multiple class instances.

    // JMX server parameters.
    private final String         host;
    private final int            registryPort;
    private final String         serviceName;

    private Class<?>             clazz                 = null;
    private Object               instance              = null;

    private Thread               registryMonitorThread = null;

    public final static String   CREATE_MBEAN_HELPER   = "createHelper";

    /**
     * Creates an instance to manage a JMX service
     * 
     * @param host The host name or IP to use
     * @param registryPort The JMX server RMI registryPort
     * @param serviceName The JMX service name
     */
    public JmxManager(String host, int registryPort, String serviceName)
    {
        this.host = host;
        this.registryPort = registryPort;
        this.serviceName = serviceName;
    }

    /**
     * Starts the JXM server.
     */
    public synchronized void start()
    {
        createRegistry(registryPort);
        startJmxConnector();
    }

    /**
     * Starts the JXM server.
     */
    public synchronized void start2()
    {
        rmiRegistry = locateDefaultRegistry();
        startServer2();
    }

    public synchronized void startDefaultRegistry()
    {
        rmiRegistry = locateDefaultRegistry();
        startServer2();

    }

    /**
     * Stops the JXM server.
     */
    public synchronized void stop()
    {
        stopRMI();
        stopJmxConnector();
    }

    protected Registry locateDefaultRegistry()
    {

        Registry registry = null;

        try
        {
            registry = LocateRegistry.getRegistry();
        }
        catch (Exception r)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Unable to locate the default registry on registryPort 1099, reason='%s'",
                            r.getMessage()));
        }

        return registry;
    }

    /**
     * Starts the rmi registry.
     */
    protected void createRegistry(int port)
    {
        // Create a registry if we don't already have one.
        if (rmiRegistry == null)
        {
            try
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Starting RMI registry on registryPort: "
                            + port);
                }
                rmiRegistry = LocateRegistry.createRegistry(port);
            }
            catch (Throwable e)
            {
                throw new ServerRuntimeException(
                        "Unable to start rmi registry on registryPort: " + port,
                        e);
            }

        }
    }

    /**
     * Deallocates the RMI registry.
     */
    protected void stopRMI()
    {
        if (rmiRegistry != null)
        {
            try
            {
                UnicastRemoteObject.unexportObject(rmiRegistry, true);
            }
            catch (NoSuchObjectException e)
            {
                logger.warn(
                        "Unexpected error while shutting down RMI registry", e);
            }
            rmiRegistry = null;
        }
    }

    /**
     * Starts the JMX connector for the server.
     */
    protected void startServer2()
    {
        String serviceAddress = null;
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            serviceAddress = "service:jmx:rmi:///jndi/rmi://" + getHostName()
                    + "/" + serviceName;

            JMXServiceURL address = new JMXServiceURL(serviceAddress);

            Map<String, String> env = new HashMap<String, String>();
            env.put(RMIConnectorServer.JNDI_REBIND_ATTRIBUTE, "true");

            JMXConnectorServer connector = JMXConnectorServerFactory
                    .newJMXConnectorServer(address, env, mbs);

            connector.start();

            if (logger.isDebugEnabled())
            {
                logger.debug("RMI Bound to: " + serviceAddress);
            }
            jmxConnectorServer = connector;
        }
        catch (Throwable e)
        {
            rmiRegistry = null;
            throw new ServerRuntimeException("Unable to create RMI listener:"
                    + getServiceProps(), e);
        }

        startRegistryMonitor();

    }

    private void startRegistryMonitor()
    {
        registryMonitorThread = new Thread(new Runnable()
        {
            public void run()
            {
                logger.info("Registry monitor is running");
                while (true)
                {
                    if (rmiRegistry != null && clazz != null
                            && instance != null)
                    {
                        try
                        {
                            Thread.sleep(3000);
                            logger.info(String.format("Re-registering %s",
                                    clazz.getSimpleName()));
                            rmiRegistry = locateDefaultRegistry();

                            if (rmiRegistry.lookup(serviceName) == null)
                            {

                            }

                            registerMBean(instance, clazz);
                        }
                        catch (InterruptedException i)
                        {

                        }
                        catch (AccessException a)
                        {
                            logger.info(String
                                    .format("Access:Exception while looking up service %s: %s",
                                            serviceName, a));

                        }
                        catch (RemoteException r)
                        {
                            // Get's here when manager is not up
                            logger.info(String
                                    .format("Remote:Exception while looking up service %s: %s",
                                            serviceName, r));
                        }
                        catch (NotBoundException n)
                        {
                            logger.info(String
                                    .format("NotBound:Exception while looking up service %s: %s",
                                            serviceName, n));

                        }
                    }
                }
            }
        });

        registryMonitorThread.start();
    }

    /**
     * Starts the JMX connector for the server.
     */
    protected void startJmxConnector()
    {
        String serviceAddress = null;
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            serviceAddress = generateServiceAddress(host, registryPort,
                    serviceName);
            JMXServiceURL address = new JMXServiceURL(serviceAddress);

            Map<String, String> env = new HashMap<String, String>();
            env.put(RMIConnectorServer.JNDI_REBIND_ATTRIBUTE, "true");
            JMXConnectorServer connector = JMXConnectorServerFactory
                    .newJMXConnectorServer(address, env, mbs);
            connector.start();

            logger.info(String.format("JMXConnector started at address %s",
                    serviceAddress));

            jmxConnectorServer = connector;
        }
        catch (Throwable e)
        {
            throw new ServerRuntimeException("Unable to create RMI listener:"
                    + getServiceProps(), e);
        }
    }

    private String getServiceProps()
    {
        return ("RMI {host=" + host + ", registryPort=" + registryPort
                + ", service=" + serviceName + "}");
    }

    /**
     * Stops the JMX connector if it is running.
     */
    protected void stopJmxConnector()
    {
        // Shut down the JMX server.
        try
        {
            if (jmxConnectorServer != null)
                jmxConnectorServer.stop();
        }
        catch (IOException e)
        {
            logger.warn("Unexpected error while shutting down JMX server", e);
        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbean The MBean instance that should be registered
     * @param mbeanInterface The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @throws ServerRuntimeException
     */
    public static void registerMBean(Object mbean, Class<?> mbeanInterface,
            String mbeanName, boolean ignored)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Registering mbean: " + mbean.getClass());

            ObjectName name = generateMBeanObjectName(mbeanInterface.getName(),
                    mbeanName);

            if (mbs.isRegistered(name))
                mbs.unregisterMBean(name);
            mbs.registerMBean(mbean, name);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException("Unable to register mbean: class="
                    + mbean.getClass() + " interface=" + mbeanInterface
                    + " name=" + mbeanName, e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbean The MBean instance that should be registered
     * @param mbeanClass The base class for the mbean
     * @throws ServerRuntimeException
     */
    public static void registerMBean(Object mbean, Class<?> mbeanClass)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Registering mbean: " + mbean.getClass());

            ObjectName name = generateMBeanObjectName(mbeanClass);

            if (mbs.isRegistered(name))
                mbs.unregisterMBean(name);
            mbs.registerMBean(mbean, name);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(String.format(
                    "Unable to register mbean for class %s because '%s'",
                    mbeanClass.getName(), e), e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbeanInterface The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @throws ServerRuntimeException
     */
    public static void unregisterMBean(Class<?> mbeanInterface, String mbeanName)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName name = generateMBeanObjectName(mbeanInterface.getName(),
                    mbeanName);
            if (mbs.isRegistered(name))
            {
                logger.info("Unregistering mbean: " + name.toString());
                mbs.unregisterMBean(name);
            }
            else
            {
                logger.warn("Ignoring attempt to unregister unknown mbean: "
                        + name.toString());
            }
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to unregister mbean: interface=" + mbeanInterface
                            + " name=" + mbeanName, e);

        }
    }

    /**
     * Server helper method to register a JMX MBean. MBeans are registered by a
     * combination of their MBean interface and the custom mbeanName argument.
     * The mbeanName permits multiple mBeans to be registered under the same
     * name.
     * 
     * @param mbeanInterface The MBean interface this instance implements
     * @throws ServerRuntimeException
     */
    public static void unregisterMBean(Class<?> mbeanInterface)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName name = generateMBeanObjectName(mbeanInterface);
            if (mbs.isRegistered(name))
            {
                logger.info("Unregistering mbean: " + name.toString());
                mbs.unregisterMBean(name);
            }
            else
            {
                logger.warn("Ignoring attempt to unregister unknown mbean: "
                        + name.toString());
            }
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to unregister mbean: interface=" + mbeanInterface,
                    e);

        }
    }

    /**
     * Client helper method to return an RMI connection. The arguments match
     * those used when instantiating the JmxManager class itself.
     * 
     * @param host the hostname to bind to in the jmx url
     * @param registryPort the registryPort number to bind to in the jmx url
     * @param serviceName the JMX service name
     * @return a connection to the server
     */
    public static JMXConnector getRMIConnector(String host, int registryPort,
            String serviceName)
    {
        String serviceAddress = null;
        try
        {
            serviceAddress = generateServiceAddress(host, registryPort,
                    serviceName);
            JMXServiceURL address = new JMXServiceURL(serviceAddress);
            JMXConnector connector = JMXConnectorFactory.connect(address, null);
            return connector;
        }
        catch (Exception e)
        {
            if (e instanceof IOException)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(
                            String.format(
                                    "A component of type '%s' at address %s:%d is not available.\n"
                                            + "Checkto be sure that the service is running.\n",
                                    serviceName, host, registryPort, e), e);
                }

                throw new ServerRuntimeException(
                        String.format(
                                "A component of type '%s' at address %s:%d is not available.\n"
                                        + "Checkto be sure that the service is running.\n",
                                serviceName, host, registryPort, e), e);
            }
            else
            {
                throw new ServerRuntimeException(String.format(
                        "Cannot establish a connection with component '%s' at address %s:%d\n"
                                + "Reason=%s\n", serviceName, host,
                        registryPort, e), e);
            }
        }
    }

    /**
     * Create a connection to the JMX server.
     * 
     * @return a connector to the JMXServer for the specified service
     */
    public JMXConnector connect(String host, int port, String serviceName)
    {
        return getRMIConnector(host, port, serviceName);
    }

    /**
     * Create a connection to the JMX server.
     * 
     * @return a connector to the JMXServer for the specified service
     */
    public static JMXConnector connect2(String host, int port,
            String serviceName)
    {
        String serviceAddress = null;
        try
        {

            serviceAddress = "service:jmx:rmi:///jndi/rmi://" + getHostName()
                    + "/" + serviceName;
            JMXServiceURL address = new JMXServiceURL(serviceAddress);
            JMXConnector connector = JMXConnectorFactory.connect(address, null);

            return connector;
        }
        catch (Throwable e)
        {
            throw new ServerRuntimeException(String.format(
                    "Unable to connect to RMI server, reason=%s", e), e);
        }
    }

    // TODO: Work in progress
    public String[] list() throws Exception
    {
        MBeanServerConnection server = ManagementFactory
                .getPlatformMBeanServer();

        Set<?> foundMBeans = server.queryMBeans(null, null);

        String[] listToReturn = new String[foundMBeans.size()];

        int i = 0;
        for (Object obj : foundMBeans)
        {
            ObjectInstance instance = (ObjectInstance) obj;
            ObjectName mbeanName = instance.getObjectName();
            listToReturn[i++] = mbeanName.toString();

        }

        return listToReturn;
    }

    /**
     * Client helper method to obtain a proxy that implements the given
     * interface by forwarding its methods through the given MBean server to the
     * named MBean.
     * 
     * @param clientConnection the MBean server to forward to
     * @param mbeanClass The MBean interface this instance implements
     * @param mbeanName A custom name for this MBean
     * @param notificationBroadcaster If true make the returned proxy implement
     *            NotificationEmitter by forwarding its methods via connection
     * @return An MBean proxy
     */
    public static Object getMBeanProxy(JMXConnector clientConnection,
            Class<?> mbeanClass, Class<?> mbeanInterface, String mbeanName,
            boolean notificationBroadcaster, boolean ignored)
    {
        try
        {

            ObjectName objectName = generateMBeanObjectName(
                    mbeanClass.getName(), mbeanName);

            return MBeanServerInvocationHandler.newProxyInstance(
                    clientConnection.getMBeanServerConnection(), objectName,
                    mbeanInterface, notificationBroadcaster);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    "Unable to get proxy connection to bean", e);
        }
    }

    /**
     * Client helper method to obtain a proxy that implements the given
     * interface by forwarding its methods through the given MBean server to the
     * named MBean.
     * 
     * @param clientConnection the MBean server to forward to
     * @param mbeanClass The class for which an MBean exists
     * @param notificationBroadcaster If true make the returned proxy implement
     *            NotificationEmitter by forwarding its methods via connection
     * @return An MBean proxy
     */
    public static Object getMBeanProxy(JMXConnector clientConnection,
            Class<?> mbeanClass, boolean notificationBroadcaster)
    {
        String mbeanInterfaceClassName = mbeanClass.getName() + "MBean";
        Class<?> mbeanInterfaceClass = null;

        try
        {
            mbeanInterfaceClass = Class.forName(mbeanInterfaceClassName);
        }
        catch (ClassNotFoundException c)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Cannot get an RMI proxy for class %s because the interface class %s was not found",
                            mbeanClass.getName(), mbeanInterfaceClassName));
        }

        try
        {
            ObjectName objectName = generateMBeanObjectName(mbeanClass);

            return MBeanServerInvocationHandler.newProxyInstance(
                    clientConnection.getMBeanServerConnection(), objectName,
                    mbeanInterfaceClass, notificationBroadcaster);
        }
        catch (Exception e)
        {
            throw new ServerRuntimeException(
                    String.format(
                            "Cannot get an RMI proxy for class %s because of this exception: %s",
                            mbeanClass.getName(), e), e);
        }
    }

    /**
     * Attach NotificationListener that can be used to listen notifications
     * emitted by MBean server.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanInterface The MBean interface this instance implements.
     * @param mbeanName A custom name for the MBean.
     * @param notificationListener User provided NotificationListener instance.
     * @throws InstanceNotFoundException
     * @throws Exception
     */
    static public void addNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanInterface, String mbeanName,
            NotificationListener notificationListener, boolean ignored)
            throws InstanceNotFoundException, Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(
                mbeanInterface.getName(), mbeanName);
        mbsc.addNotificationListener(objectName, notificationListener, null,
                null);
    }

    public static MBeanServerConnection getServerConnection(
            JMXConnector jmxConnector) throws Exception
    {
        return jmxConnector.getMBeanServerConnection();
    }

    /**
     * Attach NotificationListener that can be used to listen notifications
     * emitted by MBean server.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanClass The class for which an MBean exists.
     * @param notificationListener User provided NotificationListener instance.
     * @throws InstanceNotFoundException
     * @throws Exception
     */
    static public void addNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanClass, NotificationListener notificationListener)
            throws InstanceNotFoundException, Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(mbeanClass);
        mbsc.addNotificationListener(objectName, notificationListener, null,
                null);
    }

    /**
     * Remove NotificationListener from this MBean.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanInterface The MBean interface this instance implements.
     * @param mbeanName A custom name for the MBean.
     * @param notificationListener Previously added NotificationListener
     *            instance.
     * @throws Exception
     */
    static public void removeNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanInterface, String mbeanName,
            NotificationListener notificationListener, boolean ignored)
            throws Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(
                mbeanInterface.getName(), mbeanName);
        mbsc.removeNotificationListener(objectName, notificationListener);
    }

    /**
     * Remove NotificationListener from this MBean.
     * 
     * @param jmxConnector The MBean server connector.
     * @param mbeanClass The class for which an MBean exists.
     * @param notificationListener Previously added NotificationListener
     *            instance.
     * @throws Exception
     */
    static public void removeNotificationListener(JMXConnector jmxConnector,
            Class<?> mbeanClass, NotificationListener notificationListener)
            throws Exception
    {
        MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();
        ObjectName objectName = generateMBeanObjectName(mbeanClass);
        mbsc.removeNotificationListener(objectName, notificationListener);
    }

    // Create a service address.addNotificationListener
    private static String generateServiceAddress(String host, int port,
            String serviceName)
    {

        String serviceAddress = "service:jmx:rmi://" + host + ":" + (port + 1)
                + "/jndi/rmi://" + host + ":" + port + "/" + serviceName;
        if (logger.isDebugEnabled())
        {
            logger.debug("Service address for mbean is: " + serviceAddress);
        }
        return serviceAddress;
    }

    // Create an MBean name.
    public static ObjectName generateMBeanObjectName(Class<?> mbeanClass)
            throws Exception
    {
        String className = mbeanClass.getName();
        String type = className;
        String domain = "default";

        int lastPeriod = className.lastIndexOf('.');
        if (lastPeriod != -1)
        {
            domain = className.substring(0, lastPeriod);
            type = className.substring(className.lastIndexOf('.') + 1);
        }

        String name = String.format("%s:type=%s", domain, type);
        ObjectName objName = new ObjectName(name);

        if (logger.isDebugEnabled())
        {
            logger.debug("ObjectName is: " + objName.toString());
        }
        return objName;
    }

    // Create an MBean name.
    public static ObjectName generateMBeanObjectName(String mbeanName,
            String typeName) throws Exception
    {
        ObjectName name = new ObjectName(mbeanName + ":type=" + typeName);
        if (logger.isDebugEnabled())
        {
            logger.debug("ObjectName is: " + name.toString());
        }
        return name;
    }

    public void handleNotification(Notification notification, Object handback)
    {

        ObjectName objectName = ((MBeanServerNotification) notification)
                .getMBeanName();

        if (logger.isDebugEnabled())
        {
            logger.debug(String.format(
                    "MBean Added to the MBean server at %s:%d, ObjectName=%s",
                    host, registryPort, objectName));
        }

    }

    public static DynamicMBeanHelper createHelper(Class<?> mbeanClass)
            throws Exception
    {
        ObjectName mbeanName = generateMBeanObjectName(mbeanClass);

        MBeanInfo info = ManagementFactory.getPlatformMBeanServer()
                .getMBeanInfo(mbeanName);

        DynamicMBeanHelper helper = new DynamicMBeanHelper(mbeanClass,
                mbeanName, info);

        return helper;

    }

    public static DynamicMBeanHelper createHelper(Class<?> mbeanClass,
            String alias) throws Exception
    {
        ObjectName mbeanName = generateMBeanObjectName(mbeanClass.getName(),
                alias);

        // ObjectName mbeanName = generateMBeanObjectName(mbeanClass);

        MBeanInfo info = ManagementFactory.getPlatformMBeanServer()
                .getMBeanInfo(mbeanName);

        DynamicMBeanHelper helper = new DynamicMBeanHelper(mbeanClass,
                mbeanName, info);

        return helper;

    }

    /**
     * Get the hostname from the local host. Returns the IP address, in textual
     * form, if no hostname can be found.
     * 
     * @return the hostname for the local host
     */
    public static String getHostName()
    {
        String hostName = "localhost";

        try
        {
            hostName = InetAddress.getLocalHost().getHostName();

        }
        catch (UnknownHostException e)
        {
            // Intentionally blank
        }

        return hostName;
    }

    public Registry getRegistry()
    {
        if (rmiRegistry == null)
            rmiRegistry = locateDefaultRegistry();
        return rmiRegistry;
    }
}
