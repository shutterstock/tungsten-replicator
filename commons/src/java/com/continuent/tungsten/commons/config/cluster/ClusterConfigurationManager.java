/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.config.cluster;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.utils.FileUtils;

public class ClusterConfigurationManager
{

    /**
     * Logger
     */
    private static Logger      logger                                = Logger.getLogger(ClusterConfigurationManager.class);

    public static String       clusterHomeName                       = null;

    public static final String QUALIFIED_SERVICE_CONFIG_FILE_PATTERN = "static-%s.properties";
    public static final String SIMPLE_SERVICE_CONFIG_FILE_PATTERN    = "static-%s_%s_%s.properties";
    public static final String DYNAMIC_SERVICE_CONFIG_FILE_PATTERN   = "dynamic-%s_%s_%s.properties";

    private String             siteName;
    private String             clusterName;

    /**
     * The source of the properties for this configuration. getClusterHome
     */
    public TungstenProperties  props                                 = null;

    private File               clusterConfigDir                      = null;
    private File               clusterConfigRootDir                  = null;

    public ClusterConfigurationManager(String siteName, String clusterName)
    {
        this.siteName = siteName;
        this.clusterName = clusterName;
    }

    /**
     * Creates a new <code>ClusterConfiguration</code> object
     * 
     * @param configFileName
     * @throws ConfigurationException
     */
    public ClusterConfigurationManager(String siteName, String clusterName,
            String configFileName) throws ConfigurationException
    {
        this(siteName, clusterName);
        load(clusterName, configFileName);
    }

    /**
     * Loads a set of resource configurations from the appropriate directory
     * according to Tungsten resource conventions. The cluster directory
     * hierarchy looks like this:
     * {clusterHome}/cluster/{clusterName}/{resourceType}
     * 
     * @param resourceType
     * @throws ConfigurationException
     */
    public synchronized Map<String, Map<String, TungstenProperties>> loadClusterConfiguration(
            ResourceType resourceType) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        Map<String, Map<String, TungstenProperties>> clusterConfigurations = new HashMap<String, Map<String, TungstenProperties>>();

        File cluster = getDir(getClusterConfigRootDirName(getClusterHome()));

        for (File foundFile : cluster.listFiles())
        {
            if (foundFile.isDirectory()
                    && !foundFile.getName().equals("global"))
            {

                Map<String, TungstenProperties> clusterConfig = loadConfiguration(
                        foundFile.getName(), resourceType);
                clusterConfigurations.put(foundFile.getName(), clusterConfig);
            }
        }

        return clusterConfigurations;

    }

    /**
     * Loads a set of resource configurations from the appropriate directory
     * according to Tungsten resource conventions. The cluster directory
     * hierarchy looks like this:
     * {clusterHome}/cluster/{clusterName}/{resourceType}
     * 
     * @throws ConfigurationException
     */
    public static synchronized Map<String, TungstenProperties> loadCompositeDataServiceConfiguration()
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        Map<String, TungstenProperties> compositeConfigurations = new HashMap<String, TungstenProperties>();

        try
        {
            File cluster = getDir(getClusterConfigRootDirName(getClusterHome()));

            for (File foundFile : cluster.listFiles())
            {
                if (foundFile.getName().equals("global"))
                {
                    for (File globalConfDir : foundFile.listFiles())
                    {
                        if (!globalConfDir.isDirectory())
                            continue;

                        TungstenProperties dsConfig = ClusterConfigurationManager
                                .loadCompositeDataServiceConfiguration(
                                        globalConfDir.getName(), globalConfDir);
                        compositeConfigurations.put(globalConfDir.getName(),
                                dsConfig);
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.warn(
                    "Failed to load composite data service configurations, reason=%s",
                    e);
        }

        return compositeConfigurations;

    }

    /**
     * Returns configurations for a set of resources of a given resourceType for
     * a given clusterName.
     * 
     * @param clusterName
     * @param resourceType
     * @throws ConfigurationException
     */
    public synchronized Map<String, TungstenProperties> loadConfiguration(
            String clusterName, ResourceType resourceType)
            throws ConfigurationException
    {

        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File resources = getDir(getClusterResourceConfigDirName(
                getClusterHome(), clusterName, resourceType));

        FilenameFilter propFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".properties");
            }
        };

        Map<String, TungstenProperties> resourceMap = new HashMap<String, TungstenProperties>();

        // Load resource information
        try
        {

            for (File resourceConf : resources.listFiles(propFilter))
            {
                InputStream is = new FileInputStream(resourceConf);
                TungstenProperties resourceProps = new TungstenProperties();
                resourceProps.load(is);

                if (resourceProps.getString("name") == null)
                {
                    throw new ConfigurationException(String.format(
                            "The file %s appears to be corrupt or empty",
                            resourceConf.getPath()));
                }
                resourceMap.put(resourceProps.getString("name"), resourceProps);
            }
        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException(
                    "Error while loading datastore properties:" + i);
        }

        return resourceMap;

    }

    /**
     * Returns configurations for a set of resources of a given resourceType for
     * a given clusterName.
     * 
     * @param clusterName
     * @param resourceType
     * @throws ConfigurationException
     */
    private static synchronized TungstenProperties loadCompositeDataServiceConfiguration(
            String compositeServiceName, File confDir)
            throws ConfigurationException
    {

        File confFile = new File(confDir, compositeServiceName
                + ".global.properties");

        if (!confFile.exists())
        {

            throw new ConfigurationException(String.format(
                    "No configuration found for composite data service: %s",
                    confFile.getAbsolutePath()));
        }

        try
        {
            InputStream is = new FileInputStream(confFile);
            TungstenProperties resourceProps = new TungstenProperties();
            resourceProps.load(is);

            if (resourceProps.getString("type") == null
                    || !resourceProps.getString("type").equals("composite"))
            {
                throw new ConfigurationException(String.format(
                        "The file %s appears to be corrupt or empty",
                        confFile.getAbsolutePath()));
            }
            return resourceProps;

        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException(
                    "Error while loading datastore properties:" + i);
        }
    }

    /**
     * Store a properties file as a resource configuration using the resource
     * configuration standards for Tungsten
     * 
     * @param resourceType
     * @param resourceProps
     * @throws ConfigurationException
     */
    public synchronized void storeClusterResourceConfig(String clusterName,
            ResourceType resourceType, TungstenProperties resourceProps)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "The 'clusterHome' property was not found in the configuration file:"
                            + getRouterPropertiesFileName(getClusterHome(),
                                    null));
        }

        String resourceDir = getClusterResourceConfigDirName(getClusterHome(),
                clusterName, resourceType);

        File resources = new File(resourceDir);

        if (!resources.isDirectory())
        {
            throw new ConfigurationException(String.format(
                    "The path indicated by the name %s must be a directory.",
                    getClusterResourceConfigDirName(getClusterHome(),
                            clusterName, resourceType)));
        }

        String outFileName = resources.getAbsolutePath() + File.separator
                + resourceProps.getString("name") + ".properties";

        store(resourceProps, outFileName);

    }

    /**
     * Delete a specific resource configuration.
     * 
     * @param clusterName
     * @param resourceType
     * @param dsName
     * @throws ConfigurationException
     */
    public void deleteClusterResourceConfig(String clusterName,
            ResourceType resourceType, String dsName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No home directory found from which to configure resources.");
        }

        File resources = getDir(getClusterResourceConfigDirName(
                getClusterHome(), clusterName, resourceType));

        String delFileName = resources.getAbsolutePath() + File.separator
                + dsName + ".properties";

        delFile(delFileName);

    }

    /**
     * Return the full pathname of a resource directory for a given cluster.
     * 
     * @param clusterHome
     */
    public static String getReplicationServiceConfigDirName(String clusterHome)
    {

        return String.format("%s/../tungsten-replicator/conf", clusterHome);
    }

    /**
     * Return the full pathname of a resource directory for a given cluster.
     * 
     * @param clusterName
     * @param resourceType
     */
    public static String getClusterResourceConfigDirName(String clusterHome,
            String clusterName, ResourceType resourceType)
    {

        return getClusterConfigRootDirName(clusterHome) + File.separator
                + clusterName + File.separator
                + resourceType.toString().toLowerCase();
    }

    public static String getClusterConfigDirName(String clusterHome,
            String clusterName)
    {
        return getGlobalConfigDirName(clusterHome) + File.separator
                + ConfigurationConstants.CLUSTER_DIR + File.separator
                + clusterName;
    }

    public static String getGlobalConfigDirName(String clusterHome)
    {
        return clusterHome + File.separator
                + ConfigurationConstants.CLUSTER_CONF_DIR;
    }

    public static String getClusterConfigRootDirName(String clusterHome)
    {
        return clusterHome + File.separator
                + ConfigurationConstants.CLUSTER_CONF_DIR + File.separator
                + ConfigurationConstants.CLUSTER_DIR;
    }

    public static String getRouterPropertiesFileName(String clusterHome,
            String clusterName)
    {
        String routerProperties = System
                .getProperty(ConfigurationConstants.TR_PROPERTIES);
        if (routerProperties == null)
        {
            logger.debug("Seeking router.proproperties using cluster.home");
            return getGlobalConfigDirName(clusterHome) + File.separator
                    + ConfigurationConstants.TR_PROPERTIES;
        }
        else
        {
            logger.debug("Seeking router.properties using router.properties");
            return routerProperties;
        }
    }

    public static String getPolicyMgrPropertiesFileName(String clusterHome,
            String clusterName)
    {
        String policyMgrProperties = System
                .getProperty(ConfigurationConstants.PM_PROPERTIES);
        if (policyMgrProperties == null)
        {
            logger.debug("Seeking policymgr.properties using cluster.home");
            return getGlobalConfigDirName(clusterHome) + File.separator
                    + ConfigurationConstants.PM_PROPERTIES;
        }
        else
        {
            logger.debug("Seeking policymgr.properties using policymgr.properties");
            return policyMgrProperties;
        }
    }

    public void createDefaultConfiguration(String clusterName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        createClusterConfigRootDirs();
        createConfigDirs(clusterName);
    }

    /**
     * writeDataServiceConfiguration - Does what is required to write a data
     * service configuration to the correct location in the cluster
     * configuration directories.
     * 
     * @param clusterName
     * @param dataServiceName
     * @param serviceProperties
     * @throws ConfigurationException
     */
    public void writeDataServiceConfiguration(String clusterName,
            String dataServiceName, TungstenProperties serviceProperties)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String dataServiceConfigDirName = getClusterResourceConfigDirName(
                getClusterHome(), clusterName, ResourceType.DATASERVICE)
                + File.separator + dataServiceName;

        File dir = new File(dataServiceConfigDirName);
        if (!dir.exists())
        {
            if (!dir.mkdir())
            {
                throw new ConfigurationException("Unable to create directory "
                        + dataServiceConfigDirName);
            }
        }

        String propFileName = dataServiceConfigDirName + File.separator
                + dataServiceName + ".global.properties";

        try
        {
            store(serviceProperties, propFileName);
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to write properties for dataservice '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * writeDataServiceConfiguration - Does what is required to write a data
     * service configuration to the correct location in the cluster
     * configuration directories.
     * 
     * @param dataServiceName
     * @param serviceProperties
     * @throws ConfigurationException
     */
    public void writeCompositeDataServiceConfiguration(String dataServiceName,
            TungstenProperties serviceProperties) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String dataServiceConfigDirName = getClusterConfigRootDirName(getClusterHome())
                + File.separator + "global" + File.separator + dataServiceName;

        File dir = new File(dataServiceConfigDirName);
        if (!dir.exists())
        {
            if (!dir.mkdirs())
            {
                throw new ConfigurationException("Unable to create directory "
                        + dataServiceConfigDirName);
            }
        }

        String propFileName = dataServiceConfigDirName + File.separator
                + dataServiceName + ".global.properties";

        try
        {
            store(serviceProperties, propFileName);
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to write properties for dataservice '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * writeReplicationServiceConfiguration - Does what is required to write a r
     * service configuration to the correct location in the cluster
     * configuration directories.
     * 
     * @param clusterName
     * @param dataServiceName
     * @param facetName
     * @param replicatorConfig
     * @param replace
     * @throws ConfigurationException
     */
    public void writeReplicationServiceConfiguration(String clusterName,
            String dataServiceName, String facetName, String replicatorConfig,
            boolean replace) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String replicationServiceConfigDir = getReplicationServiceConfigDirName(getClusterHome());

        File dir = new File(replicationServiceConfigDir);
        if (!dir.exists())
        {
            if (!dir.mkdir())
            {
                throw new ConfigurationException("Unable to create directory "
                        + replicationServiceConfigDir);
            }
        }

        String servicePropFileName = replicationServiceConfigDir
                + File.separator
                + String.format(QUALIFIED_SERVICE_CONFIG_FILE_PATTERN,
                        dataServiceName);

        try
        {
            write(replicatorConfig, servicePropFileName, replace);
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to write properties for replication service '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * writeReplicationServiceConfiguration - Does what is required to write a r
     * service configuration to the correct location in the cluster
     * configuration directories.
     * 
     * @param configuration
     * @throws ConfigurationException
     */
    public static void writeClusterConfiguration(String configuration)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String clusterConfigDir = getClusterConfigRootDirName(getClusterHome());

        File dir = new File(clusterConfigDir);
        if (!dir.exists())
        {

            throw new ConfigurationException("Directory does not exist:"
                    + clusterConfigDir);

        }

        String clustersConfigFileName = clusterConfigDir + File.separator
                + "clusters.properties";
        try
        {
            write(configuration, clustersConfigFileName, true);
        }
        catch (Exception e)
        {
            String message = String.format(
                    "Unable to write cluster configuration, reason=%s", e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * deleteDataServiceConfiguration - deletes an existing data service
     * configuration.
     * 
     * @param siteName TODO
     * @param clusterName
     * @param dataServiceName
     * @throws ConfigurationException
     */
    public void deleteReplicationServiceConfiguration(String siteName,
            String clusterName, String dataServiceName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String replicationServiceConfigDir = getReplicationServiceConfigDirName(getClusterHome());

        File dir = new File(replicationServiceConfigDir);
        if (!dir.exists())
        {

            throw new ConfigurationException("Directory does not exist: "
                    + replicationServiceConfigDir);

        }

        String propFileName = replicationServiceConfigDir
                + File.separator
                + String.format(SIMPLE_SERVICE_CONFIG_FILE_PATTERN, siteName,
                        clusterName, dataServiceName);

        String dynamicPropsFileName = replicationServiceConfigDir
                + File.separator
                + String.format(DYNAMIC_SERVICE_CONFIG_FILE_PATTERN, siteName,
                        clusterName, dataServiceName);

        File propsFile = new File(propFileName);
        File dynamicPropsFile = new File(dynamicPropsFileName);

        if (!propsFile.exists() || !propsFile.canWrite())
        {
            throw new ConfigurationException(String.format(
                    "File '%s' does not exist or is not writeable.",
                    propFileName));
        }

        try
        {
            propsFile.delete();
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to delete properties for replication service '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }

        try
        {
            dynamicPropsFile.delete();
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to delete dynamic properties for replication service '%s', reason=%s",
                            dataServiceName, e);
            logger.warn(message, e);
        }
    }

    /**
     * deleteDataServiceConfiguration - deletes an existing data service
     * configuration.
     * 
     * @param clusterName
     * @param dataServiceName
     * @throws ConfigurationException
     */
    public void deleteDataServiceConfiguration(String clusterName,
            String dataServiceName) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String dataServiceConfigDirName = getClusterResourceConfigDirName(
                getClusterHome(), clusterName, ResourceType.DATASERVICE)
                + File.separator + dataServiceName;

        String propFileName = dataServiceConfigDirName + File.separator
                + dataServiceName + ".global.properties";

        File propsFile = new File(propFileName);

        if (!propsFile.exists() || !propsFile.canWrite())
        {
            throw new ConfigurationException(String.format(
                    "Directory '%s' does not exist or is not writeable.",
                    propFileName));
        }

        try
        {
            propsFile.delete();
            File propsDir = new File(dataServiceConfigDirName);
            propsDir.delete();
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to delete properties for dataservice '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * deleteDataServiceConfiguration - deletes an existing data service
     * configuration.
     * 
     * @param dataServiceName
     * @throws ConfigurationException
     */
    public void deleteCompositeDataServiceConfiguration(String dataServiceName)
            throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File cluster = getDir(getClusterConfigRootDirName(getClusterHome()));
        String dataServiceConfigDirName = cluster.getAbsolutePath()
                + File.separator + "global" + File.separator + dataServiceName;

        File confDir = new File(dataServiceConfigDirName);

        if (!confDir.exists() || !confDir.canWrite())
        {
            throw new ConfigurationException(String.format(
                    "Directory '%s' does not exist or is not writeable.",
                    dataServiceConfigDirName));
        }

        try
        {
            FileUtils.removeDirectory(dataServiceConfigDirName);
        }
        catch (Exception e)
        {
            String message = String
                    .format("Unable to delete properties for dataservice '%s', reason=%s",
                            dataServiceName, e);
            logger.error(message, e);
            throw new ConfigurationException(message);
        }
    }

    /**
     * Returns configurations for a set of resources of a given resourceType for
     * a given clusterName.
     * 
     * @param clusterName
     * @throws ConfigurationException
     */
    public static synchronized Map<String, TungstenProperties> loadDataServices(
            String clusterName) throws ConfigurationException
    {

        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        File dataServices = getDir(getClusterResourceConfigDirName(
                getClusterHome(), clusterName, ResourceType.DATASERVICE));

        FilenameFilter propFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".properties");
            }
        };

        FileFilter readableDirFilter = new FileFilter()
        {
            public boolean accept(File theFile)
            {
                return theFile.isDirectory() && theFile.canRead();
            }
        };

        Map<String, TungstenProperties> dataServicesMap = new HashMap<String, TungstenProperties>();

        // Load resource information
        try
        {
            for (File dataServiceDir : dataServices
                    .listFiles(readableDirFilter))
            {
                for (File resourceConf : dataServiceDir.listFiles(propFilter))
                {
                    InputStream is = new FileInputStream(resourceConf);
                    TungstenProperties resourceProps = new TungstenProperties();
                    resourceProps.load(is);

                    if (resourceProps.getString("fqn") == null)
                    {
                        throw new ConfigurationException(String.format(
                                "The file %s appears to be corrupt or empty",
                                resourceConf.getPath()));
                    }
                    dataServicesMap.put(resourceProps.getString("fqn"),
                            resourceProps);
                }
            }
        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException(
                    "Error while loading datastore properties:" + i);
        }

        return dataServicesMap;

    }

    /**
     * writeDataServiceConfiguration - Does what is required to write a data
     * service configuration to the correct location in the cluster
     * configuration directories.
     * 
     * @param clusterName
     * @param dataServiceName
     * @throws ConfigurationException
     */
    public TungstenProperties loadDataServiceConfiguration(String clusterName,
            String dataServiceName) throws ConfigurationException
    {
        if (getClusterHome() == null)
        {
            throw new ConfigurationException(
                    "No cluster.home found from which to configure cluster resources.");
        }

        String dataServiceConfigDirName = getClusterResourceConfigDirName(
                getClusterHome(), clusterName, ResourceType.DATASERVICE);

        File dir = new File(dataServiceConfigDirName);
        if (!dir.exists())
        {

            throw new ConfigurationException(String.format(
                    "Directory '%s' does not exist.", dataServiceConfigDirName));

        }

        String propFileName = dataServiceConfigDirName + File.separator
                + dataServiceName + ".properties";
        File propsFile = new File(propFileName);

        if (!propsFile.exists() || !propsFile.canRead())
        {
            throw new ConfigurationException(
                    String.format(
                            "Dataservice properties '%s' does not exist or is not readable.",
                            propFileName));
        }

        try
        {
            InputStream is = new FileInputStream(propFileName);
            TungstenProperties dataServiceProps = new TungstenProperties();
            dataServiceProps.load(is);
            return dataServiceProps;
        }
        catch (Exception e)
        {
            throw new ConfigurationException(
                    String.format(
                            "Unable to load the configuration for dataservice '%s.%s', reason=%s",
                            clusterName, dataServiceName, e));
        }

    }

    /**
     * Creates the directory hierarchy for the root of a cluster configuration
     * 
     * @throws ConfigurationException
     */
    public void createClusterConfigRootDirs() throws ConfigurationException
    {

        clusterConfigDir = new File(
                ClusterConfigurationManager
                        .getClusterConfigRootDirName(clusterHomeName));
        clusterConfigRootDir = new File(
                getClusterConfigRootDirName(clusterHomeName));

        // Ensure the generic 'cluster' exists.
        if (!clusterConfigDir.exists())
        {
            logger.debug("Creating new 'cluster' directory: "
                    + clusterConfigDir.getAbsolutePath());
            clusterConfigDir.mkdirs();
        }

        if (!clusterConfigDir.isDirectory() || !clusterConfigDir.canWrite())
        {
            throw new ConfigurationException(
                    "'cluster' directory invalid or unreadable: "
                            + clusterConfigDir.getAbsolutePath());
        }

        // Ensure the root level 'cluster' directory exists.
        if (!clusterConfigRootDir.exists())
        {
            logger.debug("Creating new cluster configuration directory: "
                    + clusterConfigRootDir.getAbsolutePath());
            clusterConfigRootDir.mkdirs();
        }

        if (!clusterConfigRootDir.isDirectory()
                || !clusterConfigRootDir.canWrite())
        {
            throw new ConfigurationException(
                    "cluster configuration directory invalid or unreadable: "
                            + clusterConfigRootDir.getAbsolutePath());
        }

    }

    public void createConfigDirs(String clusterName)
            throws ConfigurationException
    {
        createClusterConfigRootDirs();

        File clusterConfigDir = new File(clusterConfigRootDir, clusterName);
        File dataSourceConfigDir = new File(clusterConfigDir,
                ResourceType.DATASOURCE.toString().toLowerCase());

        // Ensure the datasource directory exists.
        if (!dataSourceConfigDir.exists())
        {
            logger.debug("Creating new datasource directory: "
                    + dataSourceConfigDir.getAbsolutePath());
            dataSourceConfigDir.mkdirs();
        }
        if (!dataSourceConfigDir.isDirectory()
                || !dataSourceConfigDir.canWrite())
        {
            throw new ConfigurationException(
                    "DataSource config directory invalid or unreadable: "
                            + dataSourceConfigDir.getAbsolutePath());
        }

        File dataServiceConfigDir = new File(clusterConfigDir,
                ResourceType.DATASERVICE.toString().toLowerCase());

        // Ensure the datasource directory exists.
        if (!dataServiceConfigDir.exists())
        {
            logger.debug("Creating new dataservice directory: "
                    + dataServiceConfigDir.getAbsolutePath());
            dataServiceConfigDir.mkdirs();
        }
        if (!dataServiceConfigDir.isDirectory()
                || !dataServiceConfigDir.canWrite())
        {
            throw new ConfigurationException(
                    "DataServoce config directory invalid or unreadable: "
                            + dataServiceConfigDir.getAbsolutePath());
        }

    }

    /**
     * Loads a cluster configuration from a file located on the classpath.
     * 
     * @param clusterName TODO
     * @param configFileName
     * @throws ConfigurationException
     */
    public void load(String clusterName, String configFileName)
            throws ConfigurationException
    {
        props = new TungstenProperties();
        InputStream is = null;
        File configFile = null;

        // Ensure cluster home name is properly set.
        clusterHomeName = System
                .getProperty(ConfigurationConstants.CLUSTER_HOME);

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must set the system property cluster.home");
        }

        // See if we have a system property corresponding to this name. If so,
        // we use that.
        String configFileNameFromProperty = System.getProperty(configFileName);
        if (configFileNameFromProperty == null)
        {
            logger.debug("Creating configuration file path from cluster.home: "
                    + getGlobalConfigDirName(clusterHomeName));
            configFile = new File(getGlobalConfigDirName(clusterHomeName),
                    configFileName);
        }
        else
        {
            logger.debug("Reading configuration file path from property: "
                    + configFileName);
            configFile = new File(configFileNameFromProperty);
        }

        // Ensure file exists and is readable.
        logger.debug("Loading config file from file: "
                + configFile.getAbsolutePath());
        if (!configFile.exists() || !configFile.canRead())
        {
            throw new ConfigurationException(
                    "Configuration file does not exist or is not readable: "
                            + configFile.getAbsolutePath());
        }

        // Read the properties.
        try
        {
            is = new FileInputStream(configFile);
        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(String.format(
                    "Cannot create an input stream for file '%s', reason=%s",
                    configFile.getAbsolutePath(), f));
        }

        try
        {
            props.load(is);
        }
        catch (IOException e)
        {
            throw new ConfigurationException(
                    "Unable to load configuration file:" + configFileName
                            + ", reason=" + e);
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
            }
        }

        props.applyProperties(this, true);
    }

    /**
     * deletes a specific file
     * 
     * @param delFileName
     * @throws ConfigurationException
     */
    public void delFile(String delFileName) throws ConfigurationException
    {
        File delFile = new File(delFileName);
        if (delFile.exists() && delFile.canWrite())
        {
            delFile.delete();
        }
        else
        {
            throw new ConfigurationException(
                    "Can't delete file because it is not writeable. File="
                            + delFileName);
        }
    }

    /**
     * Validates that a directory exists.
     * 
     * @param dirName
     * @throws ConfigurationException
     */
    public static File getDir(String dirName) throws ConfigurationException
    {
        File dir = new File(dirName);
        if (!dir.isDirectory())
        {
            throw new ConfigurationException(String.format(
                    "The path indicated by %s must be a directory.",
                    dir.getAbsolutePath()));
        }

        return dir;
    }

    /**
     * Stores a configuration file in a specific output file.
     * 
     * @param props
     * @param outFileName
     * @param overWriteExisting TODO
     * @throws ConfigurationException
     */
    public static void write(String props, String outFileName,
            boolean overWriteExisting) throws ConfigurationException
    {
        try
        {
            File checkFile = new File(outFileName);
            File backupFile = new File(outFileName + ".bak");

            if (checkFile.exists())
            {
                if (!overWriteExisting)
                {
                    logger.info(String
                            .format("Configuration file %s already exists - leaving existing configuration.",
                                    checkFile.getAbsolutePath()));
                    return;
                }
                else
                {

                    logger.info(String.format(
                            "Backing up existing configuration file %s.",
                            checkFile.getAbsolutePath()));

                }
                if (backupFile.exists())
                {
                    backupFile.delete();
                }

                checkFile.renameTo(new File(outFileName + ".bak"));
            }
            FileOutputStream fout = new FileOutputStream(outFileName);
            fout.write(props.getBytes(), 0, props.length());
            fout.flush();
            fout.close();

        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException("Error while storing properties:"
                    + i);
        }
    }

    /**
     * Stores a configuration file in a specific output file.
     * 
     * @param props
     * @param outFileName
     * @throws ConfigurationException
     */
    public void store(TungstenProperties props, String outFileName)
            throws ConfigurationException
    {
        try
        {
            File checkFile = new File(outFileName);
            File backupFile = new File(outFileName + ".bak");

            if (checkFile.exists())
            {
                if (backupFile.exists())
                {
                    backupFile.delete();
                }

                checkFile.renameTo(new File(outFileName + ".bak"));
            }
            FileOutputStream fout = new FileOutputStream(outFileName);
            props.store(fout);
            fout.flush();
            fout.getFD().sync();
            fout.close();

        }
        catch (FileNotFoundException f)
        {
            throw new ConfigurationException(
                    "Unable to process a file when configuring resources:" + f);
        }
        catch (IOException i)
        {
            throw new ConfigurationException("Error while storing properties:"
                    + i);
        }
    }

    /**
     * Apply the properties from this configuration to another instance.
     * 
     * @param o
     */
    public void applyProperties(Object o)
    {
        this.props.applyProperties(o);
    }

    public static String getClusterHome() throws ConfigurationException
    {

        if (clusterHomeName == null)
        {
            // Try to resolve it from a system property
            clusterHomeName = System
                    .getProperty(ConfigurationConstants.CLUSTER_HOME);
        }

        if (clusterHomeName == null)
        {
            throw new ConfigurationException(
                    "You must have the system property cluster.home set.");
        }

        return clusterHomeName;
    }

    public void setClusterHome(String chome)
    {
        clusterHomeName = chome;
    }

    /**
     * Returns the cluster properties
     * 
     * @return Returns the cluster properties.
     */
    public TungstenProperties getProps()
    {
        return props;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    /**
     * Returns the siteName value.
     * 
     * @return Returns the siteName.
     */
    public String getSiteName()
    {
        return siteName;
    }

    /**
     * Sets the siteName value.
     * 
     * @param siteName The siteName to set.
     */
    public void setSiteName(String siteName)
    {
        this.siteName = siteName;
    }

}
