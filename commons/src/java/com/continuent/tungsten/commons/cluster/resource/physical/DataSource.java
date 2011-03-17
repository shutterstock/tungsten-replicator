
package com.continuent.tungsten.commons.cluster.resource.physical;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.logical.DataShardFacetRole;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.patterns.order.HighWaterResource;
import com.continuent.tungsten.commons.patterns.order.Sequence;
import com.continuent.tungsten.commons.utils.CLUtils;

public class DataSource extends Resource implements Serializable
{
    private static final long           serialVersionUID               = 8153881753668230575L;
    private static final Logger         logger                         = Logger
                                                                               .getLogger(DataSource.class);

    public static final String          APPLIED_LATENCY                = "appliedLatency";
    public static final String          CLUSTER                        = "cluster";
    public static final String          DATASERVICE                    = "dataService";
    public static final String          DISKLOGDIR                     = "diskLogDir";
    public static final String          DRIVER                         = "driver";
    public static final String          HIGHWATER                      = "highWater";
    public static final String          HOST                           = "host";
    public static final String          ISAVAILABLE                    = "isAvailable";
    public static final String          LASTERROR                      = "lastError";
    public static final String          LASTSHUNREASON                 = "lastShunReason";
    public static final String          LOGDIR                         = "logDir";
    public static final String          LOGPATTERN                     = "logPattern";
    public static final String          NAME                           = "name";
    public static final String          PASSWORD                       = "password";
    public static final String          PORT                           = "port";
    public static final String          PRECEDENCE                     = "precedence";
    public static final String          REPLICATOR_HOST                = "replicatorHost";
    public static final String          ROLE                           = "role";
    public static final String          STATE                          = "state";
    public static final String          URL                            = "url";
    public static final String          USER                           = "user";
    public static final String          VENDOR                         = "vendor";

    // Defaults
    public static final double          DEFAULT_APPLIED_LATENCY        = 0.0;

    /**
     * The following six properties are the absolute minimum that are required
     * in order to derive a datasource that will work within the rest of the
     * framework. In particular, if 'host' is missing - it refers to the host
     * where the datasource is resident - the framework that associates
     * replicators with datasources will fail to work.
     */
    private String                      cluster                        = "";
    private String                      host                           = "";
    private String                      replicatorHost                 = "";
    private DataShardFacetRole          role                           = DataShardFacetRole.undefined;
    private String                      vendor                         = "";
    private String                      driver                         = "";
    private String                      url                            = "";

    private String                      user                           = "";
    private String                      password                       = "";

    private int                         port                           = -1;

    private String                      logPattern                     = null;
    private String                      logDir                         = null;
    private String                      diskLogDir                     = null;

    private int                         precedence                     = 0;
    private boolean                     isAvailable                    = false;

    private ResourceState               state                          = ResourceState.UNKNOWN;

    private String                      lastError                      = "";
    private String                      lastShunReason                 = "";

    private double                      appliedLatency                 = DEFAULT_APPLIED_LATENCY;
    private transient Date              lastUpdate                     = new Date();

    private transient HighWaterResource highWater                      = new HighWaterResource();

    // Statistics.
    private transient AtomicLong        activeConnectionCount          = new AtomicLong(
                                                                               0);
    private transient AtomicLong        connectionsCreatedCount        = new AtomicLong(
                                                                               0);
    private transient AtomicLong        statementsCreatedCount         = new AtomicLong(
                                                                               0);
    private transient AtomicLong        preparedStatementsCreatedCount = new AtomicLong(
                                                                               0);
    private transient AtomicLong        callableStatementsCreatedCount = new AtomicLong(
                                                                               0);

    /**
     * Represents a single life cycle transition for this datasource. A
     * transition occurs for any update() in which
     */
    private Sequence                    sequence                       = new Sequence();

    private AtomicInteger               enabled                        = new AtomicInteger(
                                                                               0);

    public DataSource(TungstenProperties props)
    {
        super(ResourceType.DATASOURCE, props.getString(DataSource.NAME,
                "unknown", true));

        applyDefaults();
        props.applyProperties(this, true);
        
        //if (props.getString(REPLICATOR_HOST) == null || props.getString(REPLICATOR_HOST).length() == 0)
        //{
        //    System.out.println("############ EMPTY REPLICATOR HOST #################");
        //    Thread.dumpStack();
        //}

        String state = props.getString(DataSource.STATE);
        /*
         * Backwards compatibility - previous versions don't have state.
         */
        if (state == null)
        {
            if (props.getBoolean(DataSource.ISAVAILABLE) == true)
                setState(ResourceState.ONLINE);
            else
                setState(ResourceState.OFFLINE);
        }
        else
        {
            setState(state);
        }
    }

    private void applyDefaults()
    {
        TungstenProperties defaultProps = toProperties();
        defaultProps.applyProperties(this, true);
    }

    public DataSource()
    {
        super(ResourceType.DATASOURCE, "unknown");
    }

    public DataSource(String key, String clusterName, String host)
    {
        super(ResourceType.DATASOURCE, key);
        this.cluster = clusterName;
        this.host = host;
    }

    static public TungstenProperties updateFromReplicatorStatus(
            TungstenProperties replicatorProps, TungstenProperties dsProps)
    {
        
        if (replicatorProps.getString(REPLICATOR_HOST) == null || replicatorProps.getString(REPLICATOR_HOST).length() == 0)
        {
            System.out.println("############ EMPTY REPLICATOR HOST #################");
            Thread.dumpStack();
        }
        
        dsProps.setString(NAME, replicatorProps.getString(Replicator.SOURCEID));
        dsProps.setString(CLUSTER, replicatorProps
                .getString(Replicator.CLUSTERNAME));
        dsProps.setString(REPLICATOR_HOST, replicatorProps
                .getString(Replicator.HOST));
        dsProps.setString(HOST, replicatorProps.getString(Replicator.DATASERVER_HOST));

        dsProps.setString(VENDOR, replicatorProps
                .getString(Replicator.RESOURCE_VENDOR));
        dsProps.setString(URL, replicatorProps
                .getString(Replicator.RESOURCE_JDBC_URL));
        dsProps.setString(DRIVER, replicatorProps
                .getString(Replicator.RESOURCE_JDBC_DRIVER));
        dsProps.setInt(PORT, replicatorProps.getInt(Replicator.RESOURCE_PORT));
        dsProps.setString(LOGDIR, replicatorProps
                .getString(Replicator.RESOURCE_LOGDIR));
        dsProps.setString(LOGPATTERN, replicatorProps
                .getString(Replicator.RESOURCE_LOGPATTERN));
        dsProps.setString(DISKLOGDIR, replicatorProps
                .getString(Replicator.RESOURCE_DISK_LOGDIR));
        dsProps.setString(REPLICATOR_HOST, replicatorProps
                .getString(Replicator.HOST));
        
        if (dsProps.getString(REPLICATOR_HOST) == null || dsProps.getString(REPLICATOR_HOST).length() == 0)
        {
            System.out.println("############ EMPTY REPLICATOR HOST #################");
            Thread.dumpStack();
        }

        return dsProps;
    }

    static public TungstenProperties createFromReplicatorStatus(
            TungstenProperties replicatorProps)
    {
        DataSource newDs = new DataSource(replicatorProps
                .getString(Replicator.SOURCEID), replicatorProps
                .getString(Replicator.CLUSTERNAME), replicatorProps
                .getString(Replicator.SOURCEID));

        newDs.setPrecedence(99);
        newDs.setIsAvailable(false);
        newDs.setState(ResourceState.OFFLINE);
        newDs.setVendor(replicatorProps.getString(Replicator.RESOURCE_VENDOR));
        newDs.setUrl(replicatorProps.getString(Replicator.RESOURCE_JDBC_URL));
        newDs.setDriver(replicatorProps
                .getString(Replicator.RESOURCE_JDBC_DRIVER));
        newDs.setPort(replicatorProps.getInt(Replicator.RESOURCE_PORT));
        newDs.setLogDir(replicatorProps.getString(Replicator.RESOURCE_LOGDIR));
        newDs.setDiskLogDir(replicatorProps
                .getString(Replicator.RESOURCE_DISK_LOGDIR));
        newDs.setLogPattern(replicatorProps
                .getString(Replicator.RESOURCE_LOGPATTERN));
        newDs.setReplicatorHost(replicatorProps
                .getString(Replicator.HOST));

        return newDs.toProperties();
    }

    public String getDriver()
    {
        if (driver == null)
            return "";

        return driver;
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public String getUrl()
    {
        if (url == null)
            return "";

        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getRole()
    {
        return role.toString();
    }

    public DataShardFacetRole getDataSourceRole()
    {
        return role;
    }

    public void setRole(String role)
    {
        this.role = DataShardFacetRole.valueOf(role.toLowerCase());
    }

    public void setDataSourceRole(DataShardFacetRole role)
    {
        this.role = role;
    }

    public int getPrecedence()
    {
        return precedence;
    }

    public void setPrecedence(int precedence)
    {
        this.precedence = precedence;
    }

    public String getVendor()
    {
        if (vendor == null)
            return "";

        return vendor;
    }

    public void setVendor(String vendor)
    {
        this.vendor = vendor;
    }

    /**
     * @return the isAvailable
     */
    public boolean isAvailable()
    {
        return isAvailable;
    }

    /**
     * @return the isAvailable
     */
    public boolean getIsAvailable()
    {
        return isAvailable;
    }

    /**
     * @param isAvailable the isDateAvailable to set
     */
    public void setIsAvailable(boolean isAvailable)
    {
        this.isAvailable = isAvailable;

        if (isAvailable)
        {
            setState(ResourceState.ONLINE);
        }
        else
        {
            setState(ResourceState.OFFLINE);
        }

        setLastShunReason("");
        setLastError("");
    }

    /**
     * Prevent the driver from pDaterocessing new connection requests. If the
     * driver is disabled, it will either cause new connection requests to wait
     * or will throw a SQLException.
     * 
     * @throws InterruptedException
     */
    public void disable() throws InterruptedException
    {
        // If waitFlag is true, we need to wait until all
        // active connections are completed.

        synchronized (enabled)
        {
            if (enabled.get() == 0)
            {
                return;
            }
            enabled.set(0);
        }
    }

    /**
     * Update a given datasource with values from a different datasource
     * 
     * @param ds
     */
    public void update(DataSource ds)
    {
        
        if (ds.getReplicatorHost() == null || ds.getReplicatorHost().length() == 0)
        {
            System.out.println("############ EMPTY REPLICATOR HOST #################");
            Thread.dumpStack();
        }
        
        synchronized (this)
        {
            sequence.next();
            setLastUpdateToNow();
            this.setName(ds.getName());
            this.setVendor(ds.getVendor());
            this.setCluster(ds.getCluster());
            this.setDriver(ds.getDriver());
            this.setUrl(ds.getUrl());
            this.setRole(ds.getRole());
            this.setPrecedence(ds.getPrecedence());
            this.setIsAvailable(ds.getIsAvailable());
            this.setState(ds.getState());
            this.setLastError(ds.getLastError());
            this.setLastShunReason(ds.getLastShunReason());
            this.setAppliedLatency(ds.getAppliedLatency());
            this.setLastError(ds.getLastError());
            this.setHighWater(ds.getHighWater());
            this.setReplicatorHost(ds.getReplicatorHost());
            this.notifyAll();
        }
    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();
        
        //if (getReplicatorHost() == null || getReplicatorHost().length() == 0)
        //{
        //    System.out.println("############ EMPTY REPLICATOR HOST #################");
        //    Thread.dumpStack();
        //}

        props.setString(NAME, getName());
        props.setString(VENDOR, getVendor());
        props.setString(CLUSTER, getCluster());
        props.setString(HOST, getHost());
        props.setString(DRIVER, getDriver());
        props.setString(URL, getUrl());
        props.setString(ROLE, getRole().toString());
        props.setInt(PRECEDENCE, getPrecedence());
        props.setBoolean(ISAVAILABLE, getIsAvailable());
        props.setString(STATE, getState().toString());
        props.setString(LASTERROR, getLastError());
        props.setString(LASTSHUNREASON, getLastShunReason());
        props.setDouble(APPLIED_LATENCY, appliedLatency);
        props.setString(REPLICATOR_HOST, getReplicatorHost());
        props.setLong("activeConnectionCount", activeConnectionCount.get());
        props.setLong("connectionsCreatedCount", connectionsCreatedCount.get());
        props.setLong("statementsCreatedCount", statementsCreatedCount.get());
        props.setLong("preparedStatementsCreatedCount",
                preparedStatementsCreatedCount.get());
        props.setLong("callableStatementsCreatedCount",
                callableStatementsCreatedCount.get());
        props.setString("highWater", highWater.toString());
        props.setString("sequence", sequence.toString());
        props.setString(LOGDIR, getLogDir());
        props.setString(LOGPATTERN, getLogPattern());
        props.setString(DISKLOGDIR, getDiskLogDir());
        props.setInt(PORT, getPort());

        return props;
    }

    /**
     * TODO: toMap definition.
     * 
     * @return properties representing this datasource
     */
    public Map<String, String> toMap()
    {
        return toProperties().hashMap();
    }

    /**
     * Creates a new <code>DataSource</code> object
     * 
     * @param dsProperties
     */
    public DataSource(Map<String, String> dsProperties)
    {
        set(dsProperties);
    }

    public void set(Map<String, String> dsProperties)
    {
        TungstenProperties props = new TungstenProperties(dsProperties);
        props.applyProperties(this, true);
    }

    /**
     * Returns the sequence value.
     * 
     * @return Returns the sequence.
     */
    public Sequence getSequence()
    {
        return sequence;
    }

    /**
     * Returns the number of currently active connections.
     */
    public long getActiveConnections()
    {
        return activeConnectionCount.get();
    }

    public void incrementActiveConnections()
    {

        long count = activeConnectionCount.incrementAndGet();
        logger.debug("Incremented connections for datasource: name="
                + this.getName() + " count=" + count);

    }

    public void decrementActiveConnections()
    {

        long count = activeConnectionCount.decrementAndGet();
        logger.debug("Decremented connections for datasource: name="
                + this.getName() + " count=" + count);

    }

    /**
     * Returns the number of connections created on this datasource.
     */
    public long getConnectionsCreated()
    {
        return connectionsCreatedCount.get();
    }

    public void incrementConnectionsCreated()
    {
        connectionsCreatedCount.incrementAndGet();
    }

    /**
     * Returns the number of JDBC Statement instances created.
     */
    public long getStatementsCreated()
    {
        return statementsCreatedCount.get();
    }

    public void incrementStatementsCreated()
    {
        this.statementsCreatedCount.incrementAndGet();
    }

    /**
     * Returns the number of JDBC PreparedStatement instances created.
     */
    public long getPreparedStatementsCreated()
    {
        return preparedStatementsCreatedCount.get();
    }

    public void incrementPreparedStatementsCreated()
    {
        this.preparedStatementsCreatedCount.incrementAndGet();
    }

    /**
     * Returns the number of JDBC CallableStatement instances created.
     */
    public long getCallableStatementsCreated()
    {
        return callableStatementsCreatedCount.get();
    }

    public void incrementCallableStatementsCreated()
    {
        this.callableStatementsCreatedCount.incrementAndGet();
    }

    /**
     * Returns the cluster value.
     * 
     * @return Returns the cluster.
     */
    public String getCluster()
    {
        return cluster;
    }

    /**
     * Sets the cluster value.
     * 
     * @param cluster The cluster to set.
     */
    public void setCluster(String cluster)
    {
        this.cluster = cluster;
    }

    /**
     * Format a datasource for display
     */
    public double getAppliedLatency()
    {
        return appliedLatency;
    }

    /**
     * Sets the last seen latency of this data source
     * 
     * @param appliedLatency update appliedLatency observed
     */
    public void setAppliedLatency(double appliedLatency)
    {
        this.appliedLatency = appliedLatency;
    }

    /**
     * Format a datasource for display
     */
    @Override
    public String toString()
    {
        return getName();
    }

    /**
     * Gives the last time this data source received an update
     * 
     * @return the last update time
     */
    public Date getLastUpdate()
    {
        return lastUpdate;
    }

    /**
     * Sets the lastUpdate field to the current instant in time
     */
    private void setLastUpdateToNow()
    {
        this.lastUpdate = new Date();
    }

    /**
     * Sets the sequence value.
     * 
     * @param sequence The sequence to set.
     */
    public void setSequence(Sequence sequence)
    {
        this.sequence = sequence;
    }

    /**
     * Returns the host value.
     * 
     * @return Returns the host.
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Sets the host value.
     * 
     * @param host The host to set.
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    public HighWaterResource getHighWater()
    {
        return highWater;
    }

    public void setHighWater(HighWaterResource highWater)
    {
        this.highWater = highWater;
    }

    public void setHighWater(long epoch, String eventId)
    {
        if (this.highWater != null)
        {
            this.highWater.update(epoch, eventId);
        }
        else
        {
            this.setHighWater(new HighWaterResource(epoch, eventId));
        }
    }

    public AtomicInteger getEnabled()
    {
        return enabled;
    }

    public void setEnabled(AtomicInteger enabled)
    {
        this.enabled = enabled;
    }

    public boolean isMaster()
    {
        return role == DataShardFacetRole.master;
    }

    public boolean isSlave()
    {
        return role == DataShardFacetRole.slave;
    }

    public ResourceState getState()
    {
        return state;
    }

    public void setState(ResourceState state)
    {
        this.state = state;
    }

    public void setState(String state)
    {
        this.state = ResourceState.valueOf(state.toUpperCase());
    }

    public void setFailed(String error)
    {
        setIsAvailable(false);

        this.state = ResourceState.FAILED;

        if (error != null)
        {
            this.lastError = error;
        }
    }

    public void setShunned(String reason)
    {
        setIsAvailable(false);

        this.state = ResourceState.SHUNNED;

        if (reason != null)
        {
            this.lastShunReason = reason;
        }
    }

    public String getLastError()
    {
        return lastError;
    }

    public void setLastError(String lastError)
    {
        this.lastError = lastError;
    }

    public String getLastShunReason()
    {
        return lastShunReason;
    }

    public void setLastShunReason(String lastShunReason)
    {
        this.lastShunReason = lastShunReason;
    }

    public static DataSource copy(DataSource ds)
    {
        return new DataSource(ds.toProperties());
    }

    /**
     * Returns the user value.
     * 
     * @return Returns the user.
     */
    public String getUser()
    {
        return user;
    }

    /**
     * Sets the user value.
     * 
     * @param user The user to set.
     */
    public void setUser(String user)
    {
        this.user = user;
    }

    /**
     * Returns the password value.
     * 
     * @return Returns the password.
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password value.
     * 
     * @param password The password to set.
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    public String describe(boolean detailed)
    {
        return CLUtils.formatStatus(this.toProperties(), null, "", false,
                detailed, false);
    }

    /**
     * Returns the logPattern value.
     * 
     * @return Returns the logPattern.
     */
    public String getLogPattern()
    {
        return logPattern;
    }

    /**
     * Sets the logPattern value.
     * 
     * @param logPattern The logPattern to set.
     */
    public void setLogPattern(String logPattern)
    {
        this.logPattern = logPattern;
    }

    /**
     * Returns the logDir value.
     * 
     * @return Returns the logDir.
     */
    public String getLogDir()
    {
        return logDir;
    }

    /**
     * Sets the logDir value.
     * 
     * @param logDir The logDir to set.
     */
    public void setLogDir(String logDir)
    {
        this.logDir = logDir;
    }

    /**
     * Returns the port value.
     * 
     * @return Returns the port.
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Sets the port value.
     * 
     * @param port The port to set.
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * Returns the diskLogDir value.
     * 
     * @return Returns the diskLogDir.
     */
    public String getDiskLogDir()
    {
        return diskLogDir;
    }

    /**
     * Sets the diskLogDir value.
     * 
     * @param diskLogDir The diskLogDir to set.
     */
    public void setDiskLogDir(String diskLogDir)
    {
        this.diskLogDir = diskLogDir;
    }

    /**
     * Returns the replicatorHost value.
     * 
     * @return Returns the replicatorHost.
     */
    public String getReplicatorHost()
    {
        return replicatorHost;
    }

    /**
     * Sets the replicatorHost value.
     * 
     * @param replicatorHost The replicatorHost to set.
     */
    public void setReplicatorHost(String replicatorHost)
    {
//        if (replicatorHost == null || replicatorHost.length() == 0)
//        {
//            System.out.println("############ EMPTY REPLICATOR HOST #################");
//            Thread.dumpStack();
//        }
        
        this.replicatorHost = replicatorHost;
    }
}
