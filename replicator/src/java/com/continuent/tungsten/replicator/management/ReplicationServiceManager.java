/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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

package com.continuent.tungsten.replicator.management;

import java.io.File;
import java.io.FilenameFilter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import javax.management.remote.JMXConnector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.physical.Replicator;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exec.ProcessExecutor;
import com.continuent.tungsten.commons.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.commons.jmx.JmxManager;
import com.continuent.tungsten.commons.jmx.MethodDesc;
import com.continuent.tungsten.commons.jmx.ParamDesc;
import com.continuent.tungsten.commons.utils.CLUtils;
import com.continuent.tungsten.commons.utils.ManifestParser;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * This class implements the main() method for launching replicator process and
 * starting all services.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicationServiceManager
        implements
            ReplicationServiceManagerMBean
{
    private static Logger                               logger                = Logger.getLogger(ReplicationServiceManager.class);
    private TungstenProperties                          serviceProps          = null;
    private TreeMap<String, OpenReplicatorManagerMBean> replicators           = new TreeMap<String, OpenReplicatorManagerMBean>();
    private Map<String, TungstenProperties>             serviceConfigurations = new TreeMap<String, TungstenProperties>();

    private int                                         masterListenPortStart = 2111;
    private int                                         masterListenPortMax   = masterListenPortStart;

    private int                                         managerRMIPort        = -1;

    private static final String                         CONFIG_FILE_PREFIX    = "static-";
    private static final String                         CONFIG_FILE_SUFFIX    = ".properties";

    /**
     * Creates a new <code>ReplicatorManager</code> object
     * 
     * @throws Exception
     */
    public ReplicationServiceManager() throws Exception
    {
    }

    /**
     * Start replicator services.
     */
    public void go() throws ReplicatorException
    {
        // Find and load the service.properties file.

        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        File propsFile = new File(confDir, "services.properties");
        serviceProps = PropertiesManager.loadProperties(propsFile);

        // Start JMX registry.
        managerRMIPort = serviceProps.getInt(ReplicatorConf.RMI_PORT,
                ReplicatorConf.RMI_DEFAULT_PORT, false);
        String rmiHost = getHostName(serviceProps);
        JmxManager jmxManager = new JmxManager(rmiHost, managerRMIPort,
                ReplicatorConf.RMI_DEFAULT_SERVICE_NAME);
        jmxManager.start();

        // Make sure we the configurations for the replicators
        // to work with.
        loadServiceConfigurations();

        Vector<TungstenProperties> remoteServices = new Vector<TungstenProperties>();

        // We will start the local services first, and only then will we start
        // remote services.
        for (String serviceName : serviceConfigurations.keySet())
        {
            TungstenProperties replProps = serviceConfigurations
                    .get(serviceName);
            String serviceType = replProps
                    .getString(ReplicatorConf.SERVICE_TYPE);
            boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

            if (serviceType.equals("local"))
            {
                // Get properties file name if specified or generate default.
                try
                {
                    logger.info(String.format(
                            "Starting the %s/%s replication service '%s'",
                            (isDetached ? "detached" : "internal"),
                            serviceType, serviceName));
                    startReplicationService(replProps);
                }
                catch (Exception e)
                {
                    logger.error(
                            "Unable to instantiate replication service: name="
                                    + serviceName, e);
                }
            }
            else if (serviceType.equals("remote"))
            {
                remoteServices.add(replProps);
            }
            else
            {
                logger.warn(String
                        .format("The replication service '%s' has an urecognized type '%s'",
                                serviceName, serviceType));
            }
        }

        for (TungstenProperties replProps : remoteServices)
        {
            String serviceName = replProps
                    .getString(ReplicatorConf.SERVICE_NAME);
            String serviceType = replProps
                    .getString(ReplicatorConf.SERVICE_TYPE);

            // Get properties file name if specified or generate default.
            try
            {
                logger.info(String.format(
                        "Starting the %s replication service '%s'",
                        serviceType, serviceName));
                startReplicationService(replProps);
            }
            catch (Exception e)
            {
                logger.error("Unable to instantiate replication service: name="
                        + serviceName, e);
            }
        }

        // Register ourselves as the master service manager bean.
        // JmxManager.registerMBean(this, ReplicationServiceManagerMBean.class);
        JmxManager.registerMBean(this, ReplicationServiceManager.class);
    }

    /**
     * Main method for ReplicatorManager.
     * 
     * @param argv
     */
    public static void main(String argv[])
    {
        ManifestParser.logReleaseWithBuildNumber(logger);
        logger.info("Starting replication service manager");

        // Parse global options and command.
        for (int i = 0; i < argv.length; i++)
        {
            String curArg = argv[i++];
            if ("-clear".equals(curArg))
            {
                System.setProperty(
                        ReplicatorRuntimeConf.CLEAR_DYNAMIC_PROPERTIES, "true");
            }
            else if ("-help".equals(curArg))
            {
                printHelp();
                System.exit(0);
            }
            else
            {
                System.err.println("Unrecognized option: " + curArg);
                System.exit(1);
            }
        }

        try
        {
            ReplicationServiceManager rmgr = new ReplicationServiceManager();
            rmgr.go();
            try
            {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException ie)
            {
                System.err.println("Interrupted");
            }
            logger.info("Stopping replication service manager");
        }
        catch (Throwable e)
        {
            logger.fatal("Unable to start replicator", e);
        }
    }

    // START OF MBEAN API

    /**
     * Returns true so that clients can confirm connection liveness.
     */
    @MethodDesc(description = "Confirm service liveness", usage = "isAlive")
    public boolean isAlive()
    {
        return true;
    }

    /**
     * Returns a list of replicators, started or not. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#services()
     */
    @MethodDesc(description = "List known replication services", usage = "services")
    public List<Map<String, String>> services() throws Exception
    {
        List<Map<String, String>> services = new ArrayList<Map<String, String>>();

        loadServiceConfigurations();

        for (String name : serviceConfigurations.keySet())
        {
            Map<String, String> info = new TreeMap<String, String>();

            info.put("name", name);

            if (replicators.get(name) != null)
            {
                info.put("started", "true");
            }
            else
            {
                info.put("started", "false");
            }
            services.add(info);
        }
        return services;
    }

    /**
     * Starts a service if it is defined. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#startService(java.lang.String)
     */
    @MethodDesc(description = "Start individual replication service", usage = "startService name")
    public boolean startService(
            @ParamDesc(name = "name", description = "service name") String name)
            throws Exception
    {
        loadServiceConfigurations();

        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }
        else if (replicators.get(name) == null)
        {
            startReplicationService(serviceConfigurations.get(name));
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Stops a service if it is started and defined. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#stopService(java.lang.String)
     */
    @MethodDesc(description = "Stop individual replication service", usage = "stopService name")
    public boolean stopService(
            @ParamDesc(name = "name", description = "service name") String name)
            throws Exception
    {
        loadServiceConfigurations();

        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }
        else if (replicators.get(name) == null)
        {
            return false;
        }
        else
        {
            stopReplicationService(name);
            return true;
        }
    }

    /**
     * Resets a replication service. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#resetService(java.lang.String)
     */
    @MethodDesc(description = "Reset individual replication service", usage = "resetService name")
    public Map<String, String> resetService(
            @ParamDesc(name = "name", description = "service name") String name)
            throws Exception
    {
        loadServiceConfigurations();

        Map<String, String> progress = new LinkedHashMap<String, String>();

        if (!serviceConfigurations.keySet().contains(name))
        {
            throw new Exception("Unknown replication service name: " + name);
        }

        TungstenProperties serviceProps = serviceConfigurations.get(name);
        TungstenProperties.substituteSystemValues(serviceProps.getProperties());

        String schemaName = serviceProps.getString("replicator.schema");
        String userName = serviceProps.getString("replicator.global.db.user");
        String password = serviceProps
                .getString("replicator.global.db.password");
        String url = serviceProps.getString("replicator.store.thl.url");

        Database db = null;

        try
        {
            db = DatabaseFactory.createDatabase(url, userName, password);
            db.connect();
            progress.put("drop schema", schemaName);
            db.dropSchema(schemaName);
            db.close();
        }
        catch (Exception e)
        {
            logger.error(String.format("Error while dropping schema %s: %s",
                    schemaName, e.getMessage()), e);
        }
        finally
        {
            if (db != null)
                db.close();
        }

        String logDirName = serviceProps
                .getString("replicator.store.thl.log_dir");
        File logDir = new File(logDirName);

        if (!removeDirectory(logDir, progress))
        {
            logger.error(String.format("Could not remove the log directory %s",
                    logDirName));
        }

        logger.info("\n" + CLUtils.formatMap("progress", progress, "", false)
                + "\n");
        return progress;

    }

    /**
     * Stops all services and terminates the replicator process. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.ReplicationServiceManagerMBean#stop()
     */
    @MethodDesc(description = "Stop replication services cleanly and exit process", usage = "stop")
    public void stop() throws Exception
    {
        for (String name : replicators.keySet())
        {
            stopReplicationService(name);
        }
        exitProcess(true, "Shutting down process after stopping services");
    }

    /**
     * Returns a list of properties that have the status for each of the current
     * services.
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean#status()
     */
    @MethodDesc(description = "Return the status for one or more replicators", usage = "status(name)")
    public Map<String, String> replicatorStatus(
            @ParamDesc(name = "name", description = "optional name of replicator") String name)
            throws Exception
    {

        OpenReplicatorManagerMBean mgr = replicators.get(name);

        if (mgr == null)
        {
            throw new Exception(String.format(
                    "There is no replication service with the name '%s'", name));
        }

        return mgr.status();
    }

    /**
     * Returns a map of status properties for all current replicators
     * 
     * @throws Exception
     */
    @MethodDesc(description = "Return the status for all current replicators", usage = "status()")
    public Map<String, String> getStatus() throws Exception
    {
        Map<String, String> managerProps = new HashMap<String, String>();

        managerProps.put(Replicator.SOURCEID,
                serviceProps.getString(ReplicatorConf.SOURCE_ID));
        managerProps.put(Replicator.STATE, ResourceState.ONLINE.toString());
        managerProps.put(Replicator.CLUSTERNAME,
                serviceProps.getString(ReplicatorConf.CLUSTER_NAME));
        managerProps.put(Replicator.HOST,
                serviceProps.getString(ReplicatorConf.REPLICATOR_HOST));
        managerProps.put(Replicator.RESOURCE_JDBC_URL,
                serviceProps.getString(ReplicatorConf.RESOURCE_JDBC_URL));
        managerProps.put(Replicator.RESOURCE_JDBC_DRIVER,
                serviceProps.getString(ReplicatorConf.RESOURCE_JDBC_DRIVER));
        managerProps.put(Replicator.RESOURCE_VENDOR,
                serviceProps.getString(ReplicatorConf.RESOURCE_VENDOR));
        managerProps.put(Replicator.RESOURCE_LOGDIR,
                serviceProps.getString(ReplicatorConf.RESOURCE_LOGDIR));
        managerProps.put(Replicator.RESOURCE_LOGPATTERN,
                serviceProps.getString(ReplicatorConf.RESOURCE_LOGPATTERN));
        managerProps.put(ReplicatorConf.RESOURCE_DISKLOGDIR,
                serviceProps.getString(ReplicatorConf.RESOURCE_DISKLOGDIR));
        managerProps.put(Replicator.RESOURCE_PORT, Integer
                .toString(serviceProps.getInt(ReplicatorConf.RESOURCE_PORT)));
        managerProps
                .put(Replicator.DATASERVER_HOST, serviceProps
                        .getString(ReplicatorConf.RESOURCE_DATASERVER_HOST));
        managerProps.put(Replicator.USER,
                serviceProps.getString(ReplicatorConf.GLOBAL_DB_USER));
        managerProps.put(Replicator.PASSWORD,
                serviceProps.getString(ReplicatorConf.GLOBAL_DB_PASSWORD));
        managerProps.put(Replicator.MAX_PORT, Integer.toString(getMaxPort()));

        Map<String, Map<String, String>> statusProps = new TreeMap<String, Map<String, String>>();

        for (String name : replicators.keySet())
        {
            statusProps.put(name, replicatorStatus(name));
        }

        managerProps.put("serviceProperties", statusProps.toString());

        return managerProps;
    }

    /**
     * Convenience method that can be visible in manager.
     * 
     * @throws Exception
     */
    @MethodDesc(description = "Return the status for all current replicators", usage = "status()")
    public Map<String, String> status() throws Exception
    {
        return getStatus();
    }

    /**
     * Terminates the replicator process immediately. Only the PID file is
     * cleaned up.
     */
    @MethodDesc(description = "Exit replicator immediately without cleanup", usage = "kill")
    public void kill() throws Exception
    {
        exitProcess(true,
                "Shutting down process immediately without stopping services");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorManager#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass());
    }

    // END OF MBEAN API

    /**
     * Utility routine to start a replication service. It will be started either
     * as an internal thread or as a detached process, depending on it's
     * configuration
     */
    private void startReplicationService(TungstenProperties replProps)
            throws ReplicatorException
    {

        String serviceName = replProps.getString(ReplicatorConf.SERVICE_NAME);
        String serviceType = replProps.getString(ReplicatorConf.SERVICE_TYPE);
        boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

        OpenReplicatorManagerMBean orm = null;

        try
        {

            if (isDetached)
            {
                orm = createDetachedService(serviceName);
            }
            else
            {
                orm = createInternalService(serviceName);
            }

            orm.start();
            int listenPort = orm.getMasterListenPort();
            if (listenPort > masterListenPortMax)
                masterListenPortMax = listenPort;

            replicators.put(serviceName, orm);

            logger.info(String.format(
                    "%s/%s replication service '%s' started successfully",
                    (isDetached ? "detached" : "internal"), serviceType,
                    serviceName));
        }
        catch (Exception e)
        {
            logger.error(String.format(
                    "Unable to start replication service '%s'", serviceName));
        }

    }

    /**
     * Creates a replication service that will run as a thread internal to the
     * ReplicationServiceManager.
     * 
     * @param serviceName
     * @return
     * @throws ReplicatorException
     */
    private OpenReplicatorManagerMBean createInternalService(String serviceName)
            throws ReplicatorException
    {
        try
        {
            OpenReplicatorManager orm = new OpenReplicatorManager(serviceName);
            orm.advertiseInternal();
            return (OpenReplicatorManagerMBean) orm;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(String.format(
                    "Unable to instantiate replication service '%s'",
                    serviceName));
        }
    }

    /**
     * Creates a replication service that will run in a separate process/JVM but
     * that can also be controlled from this manager.
     * 
     * @param serviceName
     * @throws Exception
     */
    private OpenReplicatorManagerMBean createDetachedService(String serviceName)
            throws Exception
    {

        TungstenProperties replProps = OpenReplicatorManager
                .getConfigurationProperties(serviceName);

        ArrayList<String> execList = new ArrayList<String>();

        int serviceRMIPort = replProps.getInt(ReplicatorConf.RMI_PORT);

        logger.info(String.format(
                "Starting replication service: name=%s, port=%d", serviceName,
                serviceRMIPort));

        execList.add("/home/edward/tungsten/tungsten-replicator/bin/repservice");
        execList.add("start");
        execList.add(serviceName);
        execList.add(Integer.toString(serviceRMIPort));

        ProcessExecutor processExecutor = new ProcessExecutor();
        processExecutor.setWorkDirectory(null); // Uses current working dir.
        processExecutor
                .setCommands(execList.toArray(new String[execList.size()]));
        processExecutor.run();

        String stdOut = processExecutor.getStdout();
        String stdErr = processExecutor.getStderr();

        if (stdOut != null)
            logger.info(stdOut);
        if (stdErr != null)
            logger.info(stdErr);

        return getReplicationServiceMBean(serviceName, serviceRMIPort);

    }

    // Utility routine to start a replication service.
    private void stopReplicationService(String name) throws Exception
    {
        logger.info("Stopping replication service: name=" + name);
        OpenReplicatorManagerMBean orm = replicators.get(name);
        try
        {
            orm.offline();
        }
        catch (Exception e)
        {
            logger.warn("Could not place service in offline state: "
                    + e.getMessage());
        }
        orm.stop();
        replicators.remove(name);
        logger.info("Replication service stopped successfully: name=" + name);
    }

    /**
     * Returns the hostname to be used to bind ports for RMI use.
     */
    private static String getHostName(TungstenProperties properties)
    {
        String defaultHost = properties.getString(ReplicatorConf.RMI_HOST);
        String hostName = System.getProperty(ReplicatorConf.RMI_HOST,
                defaultHost);
        // No value provided, retrieve from environment.
        if (hostName == null)
        {
            try
            {
                // Get hostname.
                InetAddress addr = InetAddress.getLocalHost();
                hostName = addr.getHostName();
            }
            catch (UnknownHostException e)
            {
                logger.info("Exception when trying to get the host name from the environment, reason="
                        + e);
            }
        }
        return hostName;
    }

    static void printHelp()
    {
        println("Tungsten Replicator Manager");
        println("Syntax:  [java " + ReplicationServiceManager.class.getName()
                + " \\");
        println("             [global-options]");
        println("Global Options:");
        println("\t-clear      Clear dynamic properties and start from defaults only");
        println("\t-help       Print help");
    }

    // Print a message to stdout.
    private static void println(String msg)
    {
        System.out.println(msg);
    }

    /**
     * Exit the process with as much clean-up as we can manage.
     * 
     * @param message
     */
    private void exitProcess(boolean ok, String message)
    {
        // Remove the PID file if it exists. Failures in this code may not
        // block the final exit call.
        logger.info(message);
        try
        {
            // Beware--no [back-]slashes allowed. Otherwise we'll fail on
            // some platforms. Use 'new File()' to concatenate file names.
            File replicatorHome = ReplicatorRuntimeConf
                    .locateReplicatorHomeDir();
            File varDir = new File(replicatorHome, "var");
            File pidFile = new File(varDir, "trep.pid");
            if (pidFile.exists())
            {
                logger.info("Removing PID file");
                pidFile.delete();
            }
        }
        catch (Throwable t)
        {
            logger.warn("Unable to complete logic to remove PID file", t);
        }

        // Exit the process.
        logger.info("Exiting process");
        if (ok)
            System.exit(0);
        else
            System.exit(1);
    }

    private void loadServiceConfigurations() throws ReplicatorException
    {
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();

        FilenameFilter serviceConfigFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith(CONFIG_FILE_PREFIX)
                        && name.endsWith(CONFIG_FILE_SUFFIX);
            }
        };

        serviceConfigurations.clear();

        // Make sure we have a list of files, sorted by name
        Map<String, File> serviceConfFiles = new TreeMap<String, File>();
        File[] fileArray = confDir.listFiles(serviceConfigFilter);
        for (File configFile : fileArray)
        {
            serviceConfFiles.put(configFile.getName(), configFile);
        }

        for (File serviceConf : serviceConfFiles.values())
        {
            // Name starts in the form static-<host>.<service>.properties
            String serviceConfName = serviceConf.getName();

            // get <host>.<service>.properties
            serviceConfName = serviceConfName.substring(serviceConfName
                    .indexOf(CONFIG_FILE_PREFIX) + CONFIG_FILE_PREFIX.length());

            // get <host>.<service>
            String baseFileName = serviceConfName.substring(0,
                    serviceConfName.indexOf(CONFIG_FILE_SUFFIX));

            // This should just be the service name.
            String serviceName = baseFileName.substring(baseFileName
                    .lastIndexOf(".") + 1);

            TungstenProperties replProps = OpenReplicatorManager
                    .getConfigurationProperties(serviceName);

            serviceConfigurations.put(serviceName, replProps);
        }

    }

    /**
     * Returns the maxPort value.
     * 
     * @return Returns the maxPort.
     */
    public int getMaxPort()
    {
        return masterListenPortMax;
    }

    /**
     * Sets the maximum listen port value for the master.
     * 
     * @param maxPort maximum port allowed
     */
    public void setMaxPort(int maxPort)
    {
        this.masterListenPortMax = maxPort;
    }

    /**
     * Returns the MBean for an open replicator. This is to be used when we
     * start an OpenReplicatorManager that is in a separate process.
     * 
     * @param serviceName
     * @param rmiPort
     * @return
     * @throws Exception
     */
    private OpenReplicatorManagerMBean getReplicationServiceMBean(
            String serviceName, int rmiPort) throws Exception
    {

        JMXConnector connection = JmxManager.getRMIConnector(
                JmxManager.getHostName(), rmiPort, serviceName);

        // Fetch MBean with service name.
        OpenReplicatorManagerMBean openReplicatorMBean = (OpenReplicatorManagerMBean) JmxManager
                .getMBeanProxy(connection, OpenReplicatorManager.class,
                        OpenReplicatorManagerMBean.class, serviceName, false,
                        false);

        return openReplicatorMBean;
    }

    /**
     * Utility function to recursively remove a directory hierarchy and all
     * files in it. This function tracks what it does by putting entries in the
     * 'progress' map passed in.
     * 
     * @param directory - directory to start at
     * @param progress - initialized map to be used to track progress.
     * @return
     */
    private boolean removeDirectory(File directory, Map<String, String> progress)
    {
        if (directory == null)
            return false;
        if (!directory.exists())
            return true;
        if (!directory.isDirectory())
            return false;

        String[] list = directory.list();

        if (list != null)
        {
            for (int i = 0; i < list.length; i++)
            {
                File entry = new File(directory, list[i]);

                if (entry.isDirectory())
                {
                    if (!removeDirectory(entry, progress))
                        return false;
                }
                else
                {
                    progress.put("delete file", entry.getName());
                    if (!entry.delete())
                        return false;
                }
            }
        }

        progress.put("delete directory", directory.getName());
        return directory.delete();
    }
}
