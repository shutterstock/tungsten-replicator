/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * 
 * This code is property of Continuent, Inc.  All rights reserved. 
 */

package com.continuent.tungsten.enterprise.replicator.shard;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.logical.DataShard;
import com.continuent.tungsten.commons.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.commons.jmx.JmxManager;
import com.continuent.tungsten.commons.jmx.MethodDesc;
import com.continuent.tungsten.commons.patterns.fsm.Action;
import com.continuent.tungsten.commons.patterns.fsm.Entity;
import com.continuent.tungsten.commons.patterns.fsm.Event;
import com.continuent.tungsten.commons.patterns.fsm.Transition;
import com.continuent.tungsten.commons.patterns.fsm.TransitionRollbackException;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.management.ExtendedActionEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Manager class for shard catalog information. Shard information is not logged.
 * This class should be declared as a service in the replicator.properties file.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EnterpriseShardManager
        implements
            EnterpriseShardManagerMBean,
            ReplicatorPlugin
{
    private static Logger          logger      = Logger
                                                       .getLogger(EnterpriseShardManager.class);

    // Service properties.
    private String                 autoInclude = "";
    private String                 autoExclude = "";

    private PluginContext          context;
    private TreeMap<String, Shard> shards      = new TreeMap<String, Shard>();

    private ShardTable             shardDb;
    private Database               db;

    // Define action classes used to deliver extended commands for sharding.

    /** List current shards. */
    class ShardListAction implements Action
    {
        private List<Shard> shardList;

        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            shardList = new ArrayList<Shard>(shards.size());
            for (String shardId : shards.keySet())
                shardList.add(shards.get(shardId));
        }

        public List<Shard> getShardList()
        {
            return shardList;
        }
    }

    /** Create a new shard. */
    class ShardCreateAction implements Action
    {
        private Shard shard;

        ShardCreateAction(Shard shard)
        {
            this.shard = shard;
        }

        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            String shardId = shard.getShardId();
            if (shards.get(shardId) == null)
            {
                shards.put(shardId, shard);
            }
            else
            {
                throw new TransitionRollbackException(
                        "Shard ID already exists: " + shardId, event, entity,
                        transition, actionType, null);
            }
        }
    }

    /** Drop an existing shard. */
    class ShardDropAction implements Action
    {
        private String  shardId;
        private boolean deleted;

        ShardDropAction(String shardId)
        {
            this.shardId = shardId;
        }

        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            deleted = (shards.remove(shardId) != null);
        }

        public boolean isDeleted()
        {
            return deleted;
        }
    }

    /** Enable/disable one or more shards. */
    class ShardSetEnabledAction implements Action
    {
        private String  shardExpression;
        private boolean enabled;

        ShardSetEnabledAction(String shardExpression, boolean enabled)
        {
            this.shardExpression = shardExpression;
            this.enabled = enabled;
        }

        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            Shard shard = shards.get(shardExpression);
            if (shard != null)
            {
                if (enabled)
                    shard.setShardState(ShardState.ENABLED);
                else
                    shard.setShardState(ShardState.DISABLED);
            }
        }
    }

    /** Detect shards. */
    class ShardDetectAction implements Action
    {
        @SuppressWarnings("unused")
        private String                 dbIncludeExpression;
        @SuppressWarnings("unused")
        private String                 dbExcludeExpression;
        private boolean                enabled;
        private TreeMap<String, Shard> newShards;

        ShardDetectAction(String dbIncludeExpression,
                String dbExcludeExpression, boolean enabled)
        {
            this.dbIncludeExpression = dbIncludeExpression;
            this.dbExcludeExpression = dbExcludeExpression;
            this.enabled = enabled;
        }

        public void doAction(Event event, Entity entity, Transition transition,
                int actionType) throws TransitionRollbackException
        {
            newShards = new TreeMap<String, Shard>();
            try
            {
                List<String> schemas = shardDb.getSchemas(db);
                for (String schema : schemas)
                {
                    if (shards.get(schema) == null)
                    {
                        Shard shard = new Shard();
                        shard.setShardId(schema);
                        if (enabled)
                            shard.setShardState(ShardState.ENABLED);
                        else
                            shard.setShardState(ShardState.DISABLED);
                        shards.put(schema, shard);
                        newShards.put(schema, shard);
                    }
                }
            }
            catch (Exception e)
            {
                throw new TransitionRollbackException(
                        "Shard detection failed: " + e.getMessage(), event,
                        entity, transition, actionType, e);
            }
        }

        public Collection<Shard> getNewShards()
        {
            return newShards.values();
        }
    }

    /**
     * Instantiates a new shard manager
     */
    public EnterpriseShardManager()
    {
    }

    public String getAutoInclude()
    {
        return autoInclude;
    }

    public void setAutoInclude(String autoInclude)
    {
        this.autoInclude = autoInclude;
    }

    public String getAutoExclude()
    {
        return autoExclude;
    }

    public void setAutoExclude(String autoExclude)
    {
        this.autoExclude = autoExclude;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.context = context;
        context.registerMBean(this, EnterpriseShardManager.class,
                "shard-manager");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        String catalogSchema = context.getReplicatorSchemaName();
        shardDb = new ShardTable(catalogSchema, "InnoDB");
        try
        {
            db = DatabaseFactory.createDatabase(context
                    .getJdbcUrl(catalogSchema), context.getJdbcUser(), context
                    .getJdbcPassword());
            db.connect(context.isRemoteService());
            shardDb.initializeShardTable(db);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize database connection", e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        db.close();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#create(java.lang.String,
     *      boolean)
     */
    public void create(String shardId, boolean enabled) throws Exception
    {
        // Create a new shard and submit the request.
        Shard shard = new Shard();
        shard.setShardId(shardId);
        if (enabled)
            shard.setShardState(ShardState.ENABLED);
        else
            shard.setShardState(ShardState.DISABLED);
        ShardCreateAction action = new ShardCreateAction(shard);
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#drop(java.lang.String)
     */
    public boolean drop(String shardExpression) throws Exception
    {
        // Create a shard drop action with the shardID and return the
        // boolean result.
        ShardDropAction action = new ShardDropAction(shardExpression);
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
        return action.isDeleted();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#detect(java.lang.String,
     *      java.lang.String, boolean)
     */
    public List<HashMap<String, String>> detect(String dbIncludeExpression,
            String dbExcludeExpression, boolean enabled) throws Exception
    {
        ShardDetectAction action = new ShardDetectAction(dbIncludeExpression,
                dbExcludeExpression, enabled);
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
        return packageShardList(action.getNewShards());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#enable(java.lang.String)
     */
    public void enable(String shardExpression) throws Exception
    {
        ShardSetEnabledAction action = new ShardSetEnabledAction(
                shardExpression, true);
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#disable(java.lang.String)
     */
    public void disable(String shardExpression) throws Exception
    {
        ShardSetEnabledAction action = new ShardSetEnabledAction(
                shardExpression, false);
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#list()
     */
    @MethodDesc(description = "Returns current shard definitions", usage = "list")
    public List<HashMap<String, String>> list() throws Exception
    {
        // Submit and process an event.
        ShardListAction action = new ShardListAction();
        ExtendedActionEvent event = new ExtendedActionEvent("ONLINE", action);
        handleEventSynchronous(event);
        List<Shard> shards = action.getShardList();
        return packageShardList(shards);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.enterprise.replicator.shard.EnterpriseShardManagerMBean#createHelper()
     */
    @MethodDesc(description = "Returns a DynamicMBeanHelper to facilitate dynamic JMX calls", usage = "createHelper")
    public DynamicMBeanHelper createHelper() throws Exception
    {
        return JmxManager.createHelper(getClass());
    }

    /**
     * Wrapper method for methods that submits a synchronous event with proper
     * MBean error handling.
     */
    private void handleEventSynchronous(Event event) throws Exception
    {
        try
        {
            context.getEventDispatcher().handleEventSynchronous(event);
        }
        catch (InterruptedException e)
        {
            // Eat the exception and show that we were interrupted.
            logger.warn("Event processing was interrupted: "
                    + event.getClass().getName());
            Thread.currentThread().interrupt();
        }
        catch (Exception e)
        {
            logger.error("Event processing failed: " + e.getMessage(), e);
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Serialize a list of shard definitions into generic types for return to
     * caller.
     */
    private List<HashMap<String, String>> packageShardList(
            Collection<Shard> shards)
    {

        List<HashMap<String, String>> mapList = new ArrayList<HashMap<String, String>>(
                shards.size());
        for (Shard shard : shards)
        {
            HashMap<String, String> map = new HashMap<String, String>();
            map.put(DataShard.NAME, shard.getShardId());
            map.put(DataShard.STATE, shard.getShardState().toString());
            mapList.add(map);
        }
        return mapList;
    }
}