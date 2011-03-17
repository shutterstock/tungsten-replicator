/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Contributor(s): Robert Hodges, Edward Archibald, Gilles Rayrat
 */

package com.continuent.tungsten.commons.config.cluster;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * The SQL-SQLRouter can be configured either by properties stored in a set of
 * properties files, appropriately organized in a directory tree, or by a single
 * XML-based configuration file of the type managed by the Tungsten Manager. In
 * any case, either set of configuration information reduces to a set of
 * properties. Routers manage connections to a <cluster> and a <cluster> is
 * composed of a set of <datasource> which, in turn, have a set of properties of
 * their own. The directory structure is: resourceHome
 * <dataSourcename>.properties <dataSourcename>.properties
 */
public class RouterConfiguration extends ClusterConfigurationManager
{
    private static Logger     logger                            = Logger
                                                                        .getLogger(RouterConfiguration.class);
    /**
     * 
     */
    private static final long serialVersionUID                  = 1L;

    /**
     * RMI service name
     */
    private String            serviceName                       = ConfigurationConstants.TR_SERVICE_NAME;
    /**
     * RMI port
     */
    private int               port                              = new Integer(
                                                                        ConfigurationConstants.TR_RMI_PORT_DEFAULT)
                                                                        .intValue();
    /**
     * RMI host
     */
    private String            host                              = ConfigurationConstants.TR_RMI_DEFAULT_HOST;

    /**
     * Indicates whether or not to enable the router on startup or to startup in
     * the disabled state.
     */
    private boolean           autoEnable                        = true;
    /**
     * Indicates whether or not to wait for active connections to disconnect
     * before disabling.
     */
    private boolean           waitForDisconnect                 = true;
    /**
     * Indicates the amount of time to wait for all connections to finish before
     * going out and forcibly closing them.
     */
    private int               waitForDisconnectTimeout          = 0;

    /**
     * Indicates whether or not we'll wait for a particular type of resource to
     * become available if it is not already.
     */
    private boolean           waitIfUnavailable                 = true;

    /**
     * The amount of time to wait, if any, for a particular type of resource to
     * become available before throwing an exception.
     */
    private int               waitIfUnavailableTimeout          = 0;

    private boolean           waitIfDisabled                    = true;

    private int               waitIfDisabledTimeout             = 0;

    /**
     * If the property is non-null, this is a class that will be loaded to
     * listen for driver notifications
     */
    private String            driverListenerClass               = "com.continuent.tungsten.commons.patterns.notification.adaptor.ResourceNotificationListenerStub";

    /**
     * The cluster member where the router is running.
     */
    private String            clusterMemberName;

    /**
     * If the property is non-null, this is a class that will be loaded tonull;
     * listen for router notifications
     */
    private String            routerListenerClass               = "com.continuent.tungsten.commons.patterns.notification.adaptor.ResourceNotificationListenerStub";

    private int               notifyPort                        = 10121;
    private String            notifierMonitorClass              = "com.continuent.tungsten.commons.patterns.notification.adaptor.ResourceNotifierStub";
    private String            dataSourceLoadBalancer_RO_RELAXED = "com.continuent.tungsten.router.resource.loadbalancer.RoundRobinSlaveLoadBalancer";

    private boolean           rrIncludeMaster                   = false;

    /** Router Gateway flag */
    private boolean           useNewProtocol                    = false;
    /** Router Gateway manager list */
    private List<String>      managerList                       = Arrays
                                                                        .asList("localhost");
    /** Router gateway listen port */
    private int               routerGatewayPort                 = Integer
                                                                        .parseInt(ConfigurationConstants.TR_GW_PORT_DEFAULT);
    private String            c3p0JMXUrl                        = "service:jmx:rmi:///jndi/rmi://localhost:3100/jmxrmi";

    public RouterConfiguration(String siteName, String clusterName)
            throws ConfigurationException
    {
        super(siteName, clusterName);

        // set up the default service values
        setPort(new Integer(ConfigurationConstants.TR_RMI_PORT_DEFAULT));
        setHost("localhost");
        setServiceName(ConfigurationConstants.TR_SERVICE_NAME);
    }

    /**
     * Loads a router configuration from disk.
     * 
     * @return a fully initialized router configuration
     * @throws ConfigurationException
     */
    public RouterConfiguration load() throws ConfigurationException
    {
        load(getClusterName(), ConfigurationConstants.TR_PROPERTIES);
        props.applyProperties(this, true);
        loadClusterDataSourceMap();
        return this;
    }

    /**
     * Loads data cluster configurations from disk.
     * 
     * @throws ConfigurationException
     */
    public synchronized Map<String, Map<String, TungstenProperties>> loadClusterDataSourceMap()
            throws ConfigurationException
    {
        return loadClusterConfiguration(ResourceType.DATASOURCE);
    }

    /**
     * Returns all of the datasource configurations for a given cluster.
     * 
     * @param clusterName
     * @return a map of TungstenProperties representing data sources for the
     *         cluster
     */
    public Map<String, TungstenProperties> getDataSourceMap(String clusterName)
    {
        Map<String, TungstenProperties> dsMap = new TreeMap<String, TungstenProperties>();

        Map<String, Map<String, TungstenProperties>> clusterDataSourceMap;

        try
        {
            if ((clusterDataSourceMap = loadClusterDataSourceMap()) != null)
            {
                Map<String, TungstenProperties> foundMap = clusterDataSourceMap
                        .get(clusterName);

                if (foundMap != null)
                {
                    // Do some quick validation to make sure that
                    // the datasource is well formed.
                    for (TungstenProperties dsProps : foundMap.values())
                    {
                        if (isValidDs(dsProps))
                        {
                            dsMap.put(dsProps.getString("name"), dsProps);
                        }
                    }

                    dsMap.putAll(foundMap);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Problem loading the datasource configuration", e);
        }

        return dsMap;
    }

    private boolean isValidDs(TungstenProperties dsToCheck)
    {
        String[] requiredProps = {"name", "vendor", "clusterName", "host",
                "driver", "url", "role", "precedence"};

        for (String prop : requiredProps)
        {
            if (dsToCheck.getString(prop) == null)
                return false;
        }

        return true;
    }

    /**
     * Returns an existing datasource configuration, if there is one, for the
     * named dataService.
     * 
     * @param clusterName
     * @param dsName
     * @return a TungstenProperties instances representing a data source
     * @throws ConfigurationException
     */
    public TungstenProperties getDataSource(String clusterName, String dsName)
            throws ConfigurationException
    {
        TungstenProperties foundDs = getDataSourceMap(clusterName).get(dsName);

        if (foundDs == null)
        {
            throw new ConfigurationException(String.format(
                    "datasource '%s' was not found in cluster '%s'", dsName,
                    clusterName));
        }

        return foundDs;
    }

    /**
     * Writes out the configuration for all datasources in the map.
     * 
     * @param clusterName
     * @param dataSourceMap
     * @throws ConfigurationException
     */
    public synchronized void storeDataSourceConfig(String clusterName,
            Map<String, TungstenProperties> dataSourceMap)
            throws ConfigurationException
    {

        for (TungstenProperties ds : dataSourceMap.values())
        {
            storeDataSourceConfig(clusterName, ds);
        }
    }

    /**
     * Writes out a single datasource configuration
     * 
     * @param clusterName
     * @param ds
     * @throws ConfigurationException
     */
    public synchronized void storeDataSourceConfig(String clusterName,
            TungstenProperties ds) throws ConfigurationException
    {
        storeClusterResourceConfig(clusterName, ResourceType.DATASOURCE, ds);
    }

    /**
     * @return the autoEnable
     */
    public boolean isAutoEnable()
    {
        return autoEnable;
    }

    /**
     * @param autoEnable the autoEnable to set
     */
    public void setAutoEnable(boolean autoEnable)
    {
        this.autoEnable = autoEnable;
        this.props.setBoolean("autoEnable", autoEnable);
    }

    /**
     * @return the waitForDisconnect
     */
    public boolean isWaitForDisconnect()
    {
        return waitForDisconnect;
    }

    /**
     * @param waitForDisconnect the waitForDisconnect to set
     */
    public void setWaitForDisconnect(boolean waitForDisconnect)
    {
        this.waitForDisconnect = waitForDisconnect;
        this.props.setBoolean("waitForDisconnect", waitForDisconnect);
    }

    /**
     * @return the waitForDisconnectTimeout
     */
    public int getWaitForDisconnectTimeout()
    {
        return waitForDisconnectTimeout;
    }

    /**
     * @param waitForDisconnectTimeout the waitForDisconnectTimeout to set
     */
    public void setWaitForDisconnectTimeout(int waitForDisconnectTimeout)
    {
        this.waitForDisconnectTimeout = waitForDisconnectTimeout;
        this.props.setInt("waitForDisconnectTimeout", waitForDisconnectTimeout);
    }

    /**
     * @return the dataSourceMap
     */
    public Map<String, Map<String, TungstenProperties>> getDataServicesMap()
            throws ConfigurationException
    {
        return loadClusterDataSourceMap();
    }

    /**
     * Returns the waitIfUnavailable value.
     * 
     * @return Returns the waitIfUnavailable.
     */
    public boolean getWaitIfUnavailable()
    {
        return waitIfUnavailable;
    }

    /**
     * Sets the waitIfUnavailable value.
     * 
     * @param waitIfUnavailable The waitIfUnavailable to set.
     */
    public void setWaitIfUnavailable(boolean waitIfUnavailable)
    {
        this.waitIfUnavailable = waitIfUnavailable;
        this.props.setBoolean("waitIfUnavailable", waitIfUnavailable);
    }

    /**
     * Returns the waitIfUnavailableTimeout value.
     * 
     * @return Returns the waitIfUnavailableTimeout.
     */
    public int getWaitIfUnavailableTimeout()
    {
        return waitIfUnavailableTimeout;
    }

    /**
     * Sets the waitIfUnavailableTimeout value.
     * 
     * @param waitIfUnavailableTimeout The waitIfUnavailableTimeout to set.
     */
    public void setWaitIfUnavailableTimeout(int waitIfUnavailableTimeout)
    {
        this.waitIfUnavailableTimeout = waitIfUnavailableTimeout;
        this.props.setInt("waitIfUnavailableTimeout", waitIfUnavailableTimeout);
    }

    /**
     * Returns the driverListenerClass value.
     * 
     * @return Returns the driverListenerClass.
     */
    public String getDriverListenerClass()
    {
        return driverListenerClass;
    }

    /**
     * Sets the driverListenerClass value.
     * 
     * @param driverListenerClass The driverListenerClass to set.
     */
    public void setDriverListenerClass(String driverListenerClass)
    {
        this.driverListenerClass = driverListenerClass;
    }

    /**
     * Returns the routerListenerClass value.
     * 
     * @return Returns the routerListenerClass.
     */
    public String getRouterListenerClass()
    {
        return routerListenerClass;
    }

    /**
     * Sets the routerListenerClass value.
     * 
     * @param routerListenerClass The routerListenerClass to set.
     */
    public void setRouterListenerClass(String routerListenerClass)
    {
        this.routerListenerClass = routerListenerClass;
    }

    public boolean isWaitIfDisabled()
    {
        return waitIfDisabled;
    }

    public void setWaitIfDisabled(boolean waitIfDisabled)
    {
        this.waitIfDisabled = waitIfDisabled;
        this.props.setBoolean("waitIfDisabled", waitIfDisabled);
    }

    public int getWaitIfDisabledTimeout()
    {
        return waitIfDisabledTimeout;
    }

    public void setWaitIfDisabledTimeout(int waitIfDisabledTimeout)
    {
        this.waitIfDisabledTimeout = waitIfDisabledTimeout;
        this.props.setInt("waitIfDisabledTimeout", waitIfDisabledTimeout);
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getNotifyPort()
    {
        return notifyPort;
    }

    public void setNotifyPort(int notifyPort)
    {
        this.notifyPort = notifyPort;
    }

    public String getNotifierMonitorClass()
    {
        return notifierMonitorClass;
    }

    public void setNotifierMonitorClass(String notifierMonitorClass)
    {
        this.notifierMonitorClass = notifierMonitorClass;
    }

    public String getDataSourceLoadBalancer_RO_RELAXED()
    {
        return dataSourceLoadBalancer_RO_RELAXED;
    }

    public void setDataSourceLoadBalancer_RO_RELAXED(
            String dataSourceLoadBalancer_RO_RELAXED)
    {
        this.dataSourceLoadBalancer_RO_RELAXED = dataSourceLoadBalancer_RO_RELAXED;
    }

    public String getClusterMemberName()
    {
        return clusterMemberName;
    }

    public void setClusterMemberName(String clusterMemberName)
    {
        this.clusterMemberName = clusterMemberName;
    }

    public boolean isRrIncludeMaster()
    {
        return rrIncludeMaster;
    }

    public void setRrIncludeMaster(boolean rrIncludeMaster)
    {
        this.rrIncludeMaster = rrIncludeMaster;
    }

    public void setUseNewProtocol(boolean useIt)
    {
        useNewProtocol = useIt;
    }

    public boolean getUseNewProtocol()
    {
        return useNewProtocol;
    }

    public void setManagerList(List<String> list)
    {
        this.managerList = list;
    }

    public List<String> getManagerList()
    {
        return managerList;
    }

    public int getRouterGatewayPort()
    {
        return routerGatewayPort;
    }

    public void setRouterGatewayPort(int port)
    {
        this.routerGatewayPort = port;
    }

    public String getC3p0JmxUrl()
    {
        return c3p0JMXUrl;
    }

    public void setC3p0JMXUrl(String url)
    {
        c3p0JMXUrl = url;
    }
}
