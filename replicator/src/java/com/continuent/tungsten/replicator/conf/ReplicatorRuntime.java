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
 * Initial developer(s): Robert Hodges
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.conf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.continuent.tungsten.commons.config.PropertyException;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.patterns.event.EventDispatcher;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.filter.FilterManualProperties;
import com.continuent.tungsten.replicator.management.OpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.Stage;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.PluginException;
import com.continuent.tungsten.replicator.plugin.PluginLoader;
import com.continuent.tungsten.replicator.plugin.PluginSpecification;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * Contains run-time data for the replicator, including properties and all
 * active plugins. The runtime is created at configuration time and is discarded
 * when the replicator goes off-line. The run-time handles basic life cycle
 * management for plug-ins, which are managed through this class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorRuntime implements PluginContext
{
    private static Logger                     logger          = Logger.getLogger(ReplicatorRuntime.class);

    /** Replicator properties. */
    private TungstenProperties                properties;

    // Replicator monitoring data.
    private ReplicatorMonitor                 replicatorMonitor;

    // Current pipeline or null if no pipeline is enabled.
    private Pipeline                          pipeline;

    // Extension classes.
    private HashMap<String, ReplicatorPlugin> extensions      = new HashMap<String, ReplicatorPlugin>();

    // Context with replicator resources like event dispatcher and JMX server.
    private OpenReplicatorContext             context;

    // Failure policies.
    private FailurePolicy                     extractorFailurePolicy;
    private FailurePolicy                     applierFailurePolicy;

    /** Replicator role. */
    private ReplicatorRole                    role;

    /** True if replicator should go online automatically at startup. */
    private boolean                           autoEnable;

    /** True if the replicator should stop on checksum failure. */
    private boolean                           consistencyFailureStop;

    /** True if the consistency check should be sensitive to column names. */
    private boolean                           consistencyCheckColumnNames;

    /** True if the consistency check should be sensitive to column types. */
    private boolean                           consistencyCheckColumnTypes;

    /** True if checksumming is on. */
    private boolean                           doChecksum;

    /** Schema name for storing replicator catalogs. */
    private String                            replicatorSchemaName;

    /** Table type, when applicable, for tungsten replicator tables */
    private String                            replicatorTableType;

    /** Source identifier for generated ReplDBMSEvents. */
    private String                            sourceId;

    /** Name of the current replicator role; used to select pipeline. */
    private String                            roleName;

    /** Cluster name to which replicator belongs. */
    private String                            clusterName;

    /** Name of the service. */
    private String                            serviceName;

    /** If true, indicates that this is a remote replication service. */
    private boolean                           remoteService;

    /** Determines whether to log replicator updates. */
    private boolean                           logSlaveUpdates = false;

    /** If true this replication service is taking over for a native slave. */
    private boolean                           nativeSlaveTakeover;

    /**
     * Creates a new Runtime instance.
     * 
     * @param properties Current system properties, which are copied rather than
     *            used directly
     */
    public ReplicatorRuntime(TungstenProperties properties,
            OpenReplicatorContext context, ReplicatorMonitor monitor)
    {
        this.properties = new TungstenProperties(properties.map());
        this.properties.trim();
        this.context = context;
        this.replicatorMonitor = monitor;
    }

    /**
     * Process configuration properties and instantiate/configure all plug-ins.
     * This method must be called before the configuration is usable.
     * 
     * @throws ReplicatorException Thrown if configuration fails
     */
    public void configure() throws ReplicatorException
    {
        // Determine the replicator role, providing a proper exception if the
        // role is not correctly set.
        roleName = properties.getString(ReplicatorConf.ROLE);
        if (ReplicatorConf.ROLE_MASTER.equals(roleName))
            role = ReplicatorRole.MASTER;
        else if (ReplicatorConf.ROLE_SLAVE.equals(roleName))
            role = ReplicatorRole.SLAVE;
        else
        {
            if (roleName == null)
            {
                throw new ReplicatorException(
                        "Property replicator.role is not set; must be name of a pipeline");
            }
            else
            {
                role = ReplicatorRole.OTHER;
            }
        }
        logger.info("Replicator role: " + roleName);

        // Set native-slave-takeover property.
        String nativeSlaveTakeoverSetting = assertPropertyDefault(
                ReplicatorConf.NATIVE_SLAVE_TAKEOVER,
                ReplicatorConf.NATIVE_SLAVE_TAKEOVER_DEFAULT);
        nativeSlaveTakeover = new Boolean(nativeSlaveTakeoverSetting);

        // Set auto-enable property.
        String autoEnableSetting = assertPropertyDefault(
                ReplicatorConf.AUTO_ENABLE, ReplicatorConf.AUTO_ENABLE_DEFAULT);
        autoEnable = new Boolean(autoEnableSetting);

        // Ensure source ID is available.
        sourceId = assertPropertyDefault(ReplicatorConf.SOURCE_ID,
                ReplicatorConf.SOURCE_ID_DEFAULT);

        // Ensure cluster name is available.
        clusterName = assertPropertyDefault(ReplicatorConf.CLUSTER_NAME,
                ReplicatorConf.CLUSTER_NAME_DEFAULT);

        // Ensure service name is available.
        serviceName = assertPropertySet(ReplicatorConf.SERVICE_NAME);
        MDC.put("serviceName", serviceName);

        // Ensure we have a valid service type.
        String serviceType = assertPropertyDefault(ReplicatorConf.SERVICE_TYPE,
                ReplicatorConf.SERVICE_TYPE_DEFAULT);
        if ("local".compareToIgnoreCase(serviceType) == 0)
            remoteService = false;
        else if ("remote".compareToIgnoreCase(serviceType) == 0)
            remoteService = true;
        else
        {
            throw new ReplicatorException("Valid settings for "
                    + ReplicatorConf.SERVICE_TYPE + " are local or remote");
        }

        // Checksum and consistency check handling.
        String checksumType = assertPropertyDefault(
                ReplicatorConf.EVENT_CHECKSUM,
                ReplicatorConf.EVENT_CHECKSUM_DEFAULT);
        if ("".equals(checksumType)
                || "none".equals(checksumType.toLowerCase()))
            doChecksum = false;
        else if ("crc".equals(checksumType.toLowerCase()))
            doChecksum = true;
        else
        {
            throw new ReplicatorException("Valid values for "
                    + ReplicatorConf.EVENT_CHECKSUM
                    + " are either 'crc' or 'none'. Found: " + checksumType);
        }

        String consistencyPolicy = assertPropertyDefault(
                ReplicatorConf.APPLIER_CONSISTENCY_POLICY,
                ReplicatorConf.APPLIER_CONSISTENCY_POLICY_DEFAULT);
        if (consistencyPolicy.compareToIgnoreCase("warn") == 0)
        {
            logger.info("Setting consistencyFailureStop to false");
            consistencyFailureStop = false;
        }
        else if (consistencyPolicy.compareToIgnoreCase("stop") == 0)
        {
            logger.info("Setting consistencyFailureStop to true");
            consistencyFailureStop = true;
        }
        else
        {
            throw new ReplicatorException("Valid values for "
                    + ReplicatorConf.APPLIER_CONSISTENCY_POLICY
                    + " are either 'stop' or 'warn'. Found: "
                    + consistencyPolicy);
        }

        String consistencyColumnNames = assertPropertyDefault(
                ReplicatorConf.APPLIER_CONSISTENCY_COL_NAMES,
                ReplicatorConf.APPLIER_CONSISTENCY_COL_NAMES_DEFAULT);
        if (consistencyColumnNames.compareToIgnoreCase("true") == 0)
        {
            logger.info("Setting consistencyCheckColumnNames to true");
            consistencyCheckColumnNames = true;
        }
        else if (consistencyColumnNames.compareToIgnoreCase("false") == 0)
        {
            logger.info("Setting consistencyCheckColumnNames to false");
            consistencyCheckColumnNames = false;
        }
        else
        {
            throw new ReplicatorException("Valid values for "
                    + ReplicatorConf.APPLIER_CONSISTENCY_COL_NAMES
                    + " are either 'true' or 'false'. Found: "
                    + consistencyColumnNames);
        }

        String consistencyColumnTypes = assertPropertyDefault(
                ReplicatorConf.APPLIER_CONSISTENCY_COL_TYPES,
                ReplicatorConf.APPLIER_CONSISTENCY_COL_TYPES_DEFAULT);
        if (consistencyColumnTypes.compareToIgnoreCase("true") == 0)
        {
            logger.info("Setting consistencyCheckColumnTypes to true");
            consistencyCheckColumnTypes = true;
        }
        else if (consistencyColumnTypes.compareToIgnoreCase("false") == 0)
        {
            logger.info("Setting consistencyCheckColumnTypes to false");
            consistencyCheckColumnTypes = false;
        }
        else
        {
            throw new ReplicatorException("Valid values for "
                    + ReplicatorConf.APPLIER_CONSISTENCY_COL_TYPES
                    + " are either 'true' or 'false'. Found: "
                    + consistencyColumnTypes);
        }

        // Store schema metadata. This default is necessary for unit tests to
        // work efficiently.
        this.replicatorSchemaName = assertPropertyDefault(
                ReplicatorConf.METADATA_SCHEMA, "tungsten_" + serviceName);

        this.replicatorTableType = assertPropertyDefault(
                ReplicatorConf.METADATA_TABLE_TYPE,
                ReplicatorConf.METADATA_TABLE_TYPE_DEFAULT);

        // See if we should log updates.
        assertPropertyDefault(ReplicatorConf.LOG_SLAVE_UPDATES,
                ReplicatorConf.LOG_SLAVE_UPDATES_DEFAULT);
        this.logSlaveUpdates = properties
                .getBoolean(ReplicatorConf.LOG_SLAVE_UPDATES);

        // Fill in THL properties.
        assertPropertyDefault(ReplicatorConf.THL_URI,
                ReplicatorConf.THL_URI_DEFAULT);
        assertPropertyDefault(ReplicatorConf.MASTER_CONNECT_URI,
                ReplicatorConf.THL_REMOTE_URI_DEFAULT);
        assertPropertyDefault(ReplicatorConf.THL_APPLIER_BLOCK_COMMIT_SIZE,
                ReplicatorConf.THL_APPLIER_BLOCK_COMMIT_SIZE_DEFAULT);

        assertPropertyDefault(ReplicatorConf.THL_PROTOCOL_BUFFER_SIZE,
                ReplicatorConf.THL_PROTOCOL_BUFFER_SIZE_DEFAULT);

        // Set default for resource JDBC URL so that unit tests run properly.
        // This value is normally set in the replicator properties.
        assertPropertyDefault(ReplicatorConf.RESOURCE_JDBC_URL,
                ReplicatorConf.RESOURCE_JDBC_URL_DEFAULT);

        // Set extractor failure policy.
        String extractorFailureSetting = assertPropertyDefault(
                ReplicatorConf.EXTRACTOR_FAILURE_POLICY,
                ReplicatorConf.EXTRACTOR_FAILURE_POLICY_DEFAULT);
        if (extractorFailureSetting.equals("stop"))
        {
            logger.info("Setting applierFailurePolicy to stop");
            extractorFailurePolicy = FailurePolicy.STOP;
        }
        else if (extractorFailureSetting.equals("warn"))
        {
            logger.info("Setting applierFailurePolicy to warn");
            extractorFailurePolicy = FailurePolicy.WARN;
        }
        else
        {
            throw new ReplicatorException("Valid values for "
                    + ReplicatorConf.EXTRACTOR_FAILURE_POLICY
                    + " are either 'stop' or 'skip'. Found: "
                    + extractorFailureSetting);
        }

        // Set applier failure policy.
        String applierFailureSetting = assertPropertyDefault(
                ReplicatorConf.APPLIER_FAILURE_POLICY,
                ReplicatorConf.APPLIER_FAILURE_POLICY_DEFAULT);
        if (applierFailureSetting.equals("stop"))
        {
            logger.info("Setting applierFailurePolicy to stop");
            applierFailurePolicy = FailurePolicy.STOP;
        }
        else if (applierFailureSetting.equals("warn"))
        {
            logger.info("Setting applierFailurePolicy to warn");
            applierFailurePolicy = FailurePolicy.WARN;
        }
        else
        {
            throw new ReplicatorException(
                    "Valid values for "
                            + ReplicatorConf.APPLIER_FAILURE_POLICY
                            + " are either 'stop' or 'skip'. Found: "
                            + properties
                                    .getString(ReplicatorConf.APPLIER_FAILURE_POLICY));

        }

        // Instantiate and configure extensions.
        instantiateExtensions();

        // Configure the pipeline.
        instantiateAndConfigurePipeline(roleName);
    }

    /**
     * Load extension classes, if defined.
     */
    protected void instantiateExtensions() throws ReplicatorException
    {
        List<String> extensionNames = properties
                .getStringList(ReplicatorConf.EXTENSIONS);
        for (String extensionName : extensionNames)
        {
            ReplicatorPlugin extension = loadAndConfigurePlugin(
                    ReplicatorConf.EXTENSION_ROOT, extensionName);
            configurePlugin(extension, this);
            extensions.put(extensionName, extension);
        }
    }

    /**
     * Instantiates a pipeline consisting of one or more stages.
     */
    protected void instantiateAndConfigurePipeline(String name)
            throws ReplicatorException
    {
        // Instantiate pipeline.
        Pipeline newPipeline = new Pipeline();
        newPipeline.setName(name);

        // If this pipeline is auto-synchronizing and set flag if so. Masters
        // default to auto sychronize; all others default to false.
        boolean autoSync = false;
        if ("master".equals(name))
            autoSync = true;

        String autoSyncProperty = ReplicatorConf.PIPELINE_ROOT + "." + name
                + ".autoSync";
        if (properties.get(autoSyncProperty) != null)
            autoSync = properties.getBoolean(autoSyncProperty);

        newPipeline.setAutoSync(autoSync);

        // Check if this pipeline synchronizes THL with its extractor.
        // Slaves default to false; others default to false.
        boolean syncTHLWithExtractor = true;
        if ("slave".equals(name))
            syncTHLWithExtractor = false;

        String syncTHLProperty = ReplicatorConf.PIPELINE_ROOT + "." + name
                + ".syncTHLWithExtractor";
        if (properties.get(syncTHLProperty) != null)
            syncTHLWithExtractor = properties.getBoolean(syncTHLProperty);

        newPipeline.setSyncTHLWithExtractor(syncTHLWithExtractor);

        // Add stores, if any.
        String storesProperty = ReplicatorConf.PIPELINE_ROOT + "." + name
                + ".stores";
        List<String> stores = properties.getStringList(storesProperty);

        for (String storeName : stores)
        {
            Store store = (Store) loadAndConfigurePlugin(
                    ReplicatorConf.STORE_ROOT, storeName);
            store.setName(storeName);
            newPipeline.addStore(storeName, store);
        }

        // Add stages.
        String stagesProperty = ReplicatorConf.PIPELINE_ROOT + "." + name;
        List<String> stages = properties.getStringList(stagesProperty);

        if (stages.size() == 0)
        {
            throw new ReplicatorException(
                    "Pipeline does not exist or has no stages: " + name);
        }

        for (String stageName : stages)
        {
            String stageProperty = ReplicatorConf.STAGE_ROOT + "." + stageName
                    + ".";
            TungstenProperties stageProps = properties.subset(stageProperty,
                    true);

            // Instantiate a stage.
            Stage stage = new Stage(newPipeline);
            stage.setName(stageName);
            newPipeline.addStage(stage);

            // Find and load extractor.
            String extractorName = stageProps.remove(ReplicatorConf.EXTRACTOR);
            if (extractorName == null)
            {
                throw new ReplicatorException(
                        "No extractor specified for stage: " + stageName);
            }
            else
            {
                // Load extractor.
                PluginSpecification extractorSpecification = specifyPlugin(
                        ReplicatorConf.EXTRACTOR_ROOT, extractorName);
                stage.setExtractorSpec(extractorSpecification);

            }

            // Find and load filters.
            List<String> filterNames = stageProps
                    .getStringList(ReplicatorConf.FILTERS);
            stageProps.remove(ReplicatorConf.FILTERS);
            List<PluginSpecification> filterSpecs = new ArrayList<PluginSpecification>();
            for (String filterName : filterNames)
            {
                PluginSpecification fps = specifyPlugin(
                        ReplicatorConf.FILTER_ROOT, filterName);
                filterSpecs.add(fps);
            }
            stage.setFilterSpecs(filterSpecs);

            // Find and load extractor.
            String applierName = stageProps.remove(ReplicatorConf.APPLIER);
            if (applierName == null)
            {
                throw new ReplicatorException(
                        "No applier specified for stage: " + stageName);
            }
            else
            {
                PluginSpecification applierSpec = specifyPlugin(
                        ReplicatorConf.APPLIER_ROOT, applierName);
                stage.setApplierSpec(applierSpec);
            }

            // Any remaining properties should be applied to the stage instance.
            stageProps.applyProperties(stage);
        }

        // Configure the pipeline and then make it visible in the runtime.
        try
        {
            newPipeline.configure(this);
        }
        catch (InterruptedException e)
        {
            // We are not really ready to handle an interruption in a
            // civilized way so we just die.
            throw new ReplicatorException(
                    "Pipeline configuration was interrupted");
        }
        pipeline = newPipeline;
    }

    /**
     * Prepares pipeline for use.
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        for (String extensionName : extensions.keySet())
        {
            logger.info("Preparing extension service for use: " + extensionName);
            extensions.get(extensionName).prepare(this);
        }
        logger.info("Preparing pipeline for use: " + pipeline.getName());
        pipeline.prepare(this);
    }

    /**
     * Releases all plug-ins stored in runtime by calling their release()
     * methods and setting the storage locations to null so they can be
     * garbage-collected. This method is idempotent and may therefore be called
     * multiple times.
     */
    public void release()
    {
        if (pipeline != null)
        {
            pipeline.release(this);
            pipeline = null;
        }
    }

    /**
     * Ensures that a required property has a default if unspecified.
     */
    protected String assertPropertyDefault(String key, String value)
    {
        if (properties.getString(key) == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Assigning default global property value: key="
                        + key + " default value=" + value);
            properties.setString(key, value);
        }
        return properties.getString(key);
    }

    protected String assertPropertySet(String key) throws ReplicatorException
    {
        String value = properties.getString(key);
        if (value == null)
            throw new ReplicatorException("Required property not set: key="
                    + key);
        else
            return value;
    }

    /**
     * Generic code to load and configure a plugin.
     */
    protected ReplicatorPlugin loadAndConfigurePlugin(String prefix, String name)
            throws ReplicatorException
    {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException(
                    "Plugin class name property is missing or null:  key="
                            + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        if (logger.isDebugEnabled())
            logger.debug("Loading plugin: key=" + pluginPrefix + " class name="
                    + pluginClassName);

        // Subset plug-in properties.
        TungstenProperties pluginProperties = properties.subset(pluginPrefix
                + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        ReplicatorPlugin plugin;
        try
        {
            plugin = PluginLoader.load(pluginClassName);
            if (plugin instanceof FilterManualProperties)
                ((FilterManualProperties) plugin).setConfigPrefix(pluginPrefix);
            else
                pluginProperties.applyProperties(plugin);
        }
        catch (PluginException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                    + pluginPrefix + " class name=" + pluginClassName, e);
        }
        catch (PropertyException e)
        {
            throw new ReplicatorException(
                    "Unable to configure plugin properties: key="
                            + pluginPrefix + " class name=" + pluginClassName
                            + " : " + e.getMessage(), e);
        }
        return plugin;
    }

    /**
     * Generic code to load and configure a plugin.
     */
    protected PluginSpecification specifyPlugin(String prefix, String name)
            throws ReplicatorException
    {
        String pluginPrefix = prefix + "." + name.trim();

        // Find the plug-in class name.
        String rawClassName = properties.getString(pluginPrefix);
        if (rawClassName == null)
            throw new ReplicatorException(
                    "Plugin class name property is missing or null:  key="
                            + pluginPrefix);
        String pluginClassName = rawClassName.trim();
        if (logger.isDebugEnabled())
            logger.debug("Loading plugin: key=" + pluginPrefix + " class name="
                    + pluginClassName);

        // Subset plug-in properties.
        TungstenProperties pluginProperties = properties.subset(pluginPrefix
                + ".", true);
        if (logger.isDebugEnabled())
            logger.debug("Plugin properties: " + pluginProperties.toString());

        // Load the plug-in class and configure its properties.
        Class<?> pluginClass;
        try
        {
            pluginClass = PluginLoader.loadClass(pluginClassName);
        }
        catch (PluginException e)
        {
            throw new ReplicatorException(
                    "Unable to instantiate plugin class: key=" + pluginPrefix
                            + " class name=" + pluginClassName, e);
        }

        return new PluginSpecification(pluginPrefix, name, pluginClass,
                pluginProperties);
    }

    /**
     * Returns the current replicator properties.
     */
    public TungstenProperties getReplicatorProperties()
    {
        return properties;
    }

    /**
     * Returns the monitoring data object.
     */
    public ReplicatorMonitor getMonitor()
    {
        return replicatorMonitor;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isConsistencyFailureStop()
     */
    public boolean isConsistencyFailureStop()
    {
        return consistencyFailureStop;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isConsistencyCheckColumnNames()
     */
    public boolean isConsistencyCheckColumnNames()
    {
        return consistencyCheckColumnNames;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isConsistencyCheckColumnTypes()
     */
    public boolean isConsistencyCheckColumnTypes()
    {
        return consistencyCheckColumnTypes;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isDoChecksum()
     */
    public boolean isDoChecksum()
    {
        return doChecksum;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getJdbcUrl(java.lang.String)
     */
    public String getJdbcUrl(String database)
    {
        // Get the JDBC URL for this data source and subst
        Properties jProps = new Properties();
        jProps.setProperty("URL",
                properties.getString(ReplicatorConf.RESOURCE_JDBC_URL));
        if (database == null)
            jProps.setProperty("DBNAME", this.getReplicatorSchemaName());
        else
            jProps.setProperty("DBNAME", database);
        TungstenProperties.substituteSystemValues(jProps);
        return jProps.getProperty("URL");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getJdbcUser()
     */
    public String getJdbcUser()
    {
        return properties.getString(ReplicatorConf.GLOBAL_DB_USER);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getJdbcPassword()
     */
    public String getJdbcPassword()
    {
        return properties.getString(ReplicatorConf.GLOBAL_DB_PASSWORD);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getReplicatorSchemaName()
     */
    public String getReplicatorSchemaName()
    {
        return replicatorSchemaName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getRoleName()
     */
    public String getRoleName()
    {
        return roleName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getSourceId()
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getClusterName()
     */
    public String getClusterName()
    {
        return clusterName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getServiceName()
     */
    public String getServiceName()
    {
        return serviceName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isSlave()
     */
    public boolean isSlave()
    {
        return (role == ReplicatorRole.SLAVE);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isMaster()
     */
    public boolean isMaster()
    {
        return (role == ReplicatorRole.MASTER);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isAutoEnable()
     */
    public boolean isAutoEnable()
    {
        return autoEnable;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getStore(java.lang.String)
     */
    public Store getStore(String name)
    {
        return pipeline.getStore(name);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getStores()
     */
    public List<Store> getStores()
    {
        ArrayList<Store> stores = new ArrayList<Store>();
        for (String name : pipeline.getStoreNames())
            stores.add(pipeline.getStore(name));
        return stores;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getEventDispatcher()
     */
    public EventDispatcher getEventDispatcher()
    {
        return context.getEventDispatcher();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#isRemoteService()
     */
    public boolean isRemoteService()
    {
        return remoteService;
    }

    /** Returns the pipeline. */
    public Pipeline getPipeline()
    {
        return pipeline;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getApplierFailurePolicy()
     */
    public FailurePolicy getApplierFailurePolicy()
    {
        return applierFailurePolicy;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getExtractorFailurePolicy()
     */
    public FailurePolicy getExtractorFailurePolicy()
    {
        return extractorFailurePolicy;
    }

    /**
     * Returns the current minimum sequence number.
     */
    public long getMinSeqno()
    {
        return pipeline.getMinStoredSeqno();
    }

    /**
     * Returns the current maximum sequence number.
     */
    public long getMaxSeqno()
    {
        return pipeline.getMaxStoredSeqno();
    }

    public String getTungstenTableType()
    {
        // TODO Auto-generated method stub
        return this.replicatorTableType;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getExtension(java.lang.String)
     */
    public ReplicatorPlugin getExtension(String name)
    {
        return extensions.get(name);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#getExtensionNames()
     */
    public List<String> getExtensionNames()
    {
        return new ArrayList<String>(extensions.keySet());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#registerMBean(Object,
     *      Class, String) java.lang.String)
     */
    public void registerMBean(Object mbean, Class<?> mbeanClass, String name)
    {
        context.registerMBean(mbean, mbeanClass, name);
    }

    /** Call configure method on a plugin class. */
    public static void configurePlugin(ReplicatorPlugin plugin,
            PluginContext context) throws ReplicatorException
    {
        String pluginClassName = plugin.getClass().getName();
        try
        {
            plugin.configure(context);
        }
        catch (ReplicatorException e)
        {
            throw new ReplicatorException(
                    "Unable to configure plugin: class name=" + pluginClassName,
                    e);
        }
        catch (Throwable t)
        {
            String message = "Unable to configure plugin: class name="
                    + pluginClassName;

            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a configured plugin.
        if (logger.isDebugEnabled())
            logger.debug("Plug-in configured successfully: class name="
                    + pluginClassName);
    }

    /** Call prepare method on a plugin class. */
    public static void preparePlugin(ReplicatorPlugin plugin,
            PluginContext context) throws ReplicatorException
    {
        String pluginClassName = plugin.getClass().getName();
        try
        {
            plugin.prepare(context);
        }
        catch (ReplicatorException e)
        {
            throw new ReplicatorException(
                    "Unable to prepare plugin: class name=" + pluginClassName,
                    e);
        }
        catch (Throwable t)
        {
            String message = "Unable to prepare plugin: class name="
                    + pluginClassName;

            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }

        // It worked. We have a prepared plugin.
        if (logger.isDebugEnabled())
            logger.debug("Plug-in prepared successfully: class name="
                    + pluginClassName);
    }

    /** Call release method on a plugin class, warning on errors. */
    public static void releasePlugin(ReplicatorPlugin plugin,
            PluginContext context)
    {
        String pluginClassName = plugin.getClass().getName();
        try
        {
            plugin.release(context);
        }
        catch (Throwable t)
        {
            logger.warn("Unable to release plugin: class name="
                    + pluginClassName, t);
        }

        if (logger.isDebugEnabled())
            logger.debug("Plug-in released successfully: class name="
                    + pluginClassName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#logReplicatorUpdates()
     */
    public boolean logReplicatorUpdates()
    {
        return (logSlaveUpdates && !isMaster());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.PluginContext#nativeSlaveTakeover()
     */
    public boolean nativeSlaveTakeover()
    {
        return nativeSlaveTakeover;
    }
}