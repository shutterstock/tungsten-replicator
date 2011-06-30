/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.shard;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.mysql.MySQLExtractor;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardFilter implements Filter
{
    private static Logger logger         = Logger.getLogger(MySQLExtractor.class);

    PluginContext         context;
    Map<String, Shard>    shards;

    Database              conn           = null;

    private String        user;
    private String        url;
    private String        password;
    private boolean       remote;
    private String        service;
    private String        schemaName;
    private String        tableType;

    private boolean       autoCreate     = true;
    private boolean       enforceHome    = false;
    private int           defaultChannel = -1;
    private boolean       criticalByDef  = false;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        schemaName = context.getReplicatorSchemaName();
        tableType = ((ReplicatorRuntime) context).getTungstenTableType();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.context = context;

        shards = new HashMap<String, Shard>();

        // Read shard catalog
        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl("tungsten_" + context.getServiceName());
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        ShardTable shardTable = new ShardTable(schemaName, tableType);

        try
        {
            List<Map<String, String>> list = shardTable.list(conn);
            for (Map<String, String> map : list)
            {
                Shard shard = new Shard(map);
                logger.warn("Adding shard " + shard.getShardId());
                shards.put(shard.getShardId(), shard);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }

        remote = context.isRemoteService();
        service = context.getServiceName();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    @Override
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        String eventShard = event.getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.SHARD_ID);

        Shard shard = shards.get(eventShard);

        if (!enforceHome)
        {
            if (logger.isDebugEnabled())
                logger.debug("Enforcing home check is disabled");

            if (autoCreate && shard == null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Auto creating shard definition for "
                            + eventShard + " with home " + service);
                updateShardCatalog(eventShard);
            }
            return event;
        }

        if (!remote)
        {
            if (logger.isDebugEnabled())
                logger.debug("Local service - not filtering events");
            return event;
        }

        if (shard == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Shard not found in shards table " + eventShard);

            // Shard does not exist : should we create it?
            if (autoCreate)
            {
                updateShardCatalog(eventShard);
                return event;
            }
            else
            {
                // Dropping event as it is part of an unknown shard and
                // autoCreate is false
                if (logger.isDebugEnabled())
                    logger.debug("Dropping event from unknown shard ("
                            + eventShard + ") as autoCreate is false");
                return null;
            }
        }
        else if (shard.getHome().equals(service))
        {
            // Shard home matches the service name, apply this event
            if (logger.isDebugEnabled())
                logger.debug("Event shard matches shard home definition. Processing event.");
            return event;
        }
        else
        {
            // Shard home does not match, discard this event
            logger.info("Event shard (" + shard.getHome()
                    + ") does not match local home (" + service
                    + "). Dropping event");
            return null;
        }
    }

    /**
     * updateShardCatalog both update the shards hold in memory as well as in shard table in database.
     * 
     * @param eventShard Id of the shard to be created
     * @throws ReplicatorException
     */
    private void updateShardCatalog(String eventShard)
            throws ReplicatorException
    {
        if (logger.isDebugEnabled())
            logger.debug("Creating unknown shard " + eventShard + " for home "
                    + service);

        ShardManager manager = new ShardManager(service, url, user, password,
                schemaName, tableType);
        List<Map<String, String>> params = new ArrayList<Map<String, String>>();
        Map<String, String> newShard = new HashMap<String, String>();
        newShard.put(ShardTable.SHARD_ID_COL, eventShard);
        newShard.put(ShardTable.SHARD_CRIT_COL, Boolean.toString(criticalByDef));
        newShard.put(ShardTable.SHARD_HOME_COL, service);
        newShard.put(ShardTable.SHARD_CHANNEL_COL, Integer.toString(defaultChannel));
        params.add(newShard);
        try
        {
            manager.insert(params);
            shards.put(eventShard, new Shard(newShard));
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
    }

    public void setEnforceHome(boolean enforceHome)
    {
        this.enforceHome = enforceHome;
    }

    public void setAutoCreate(boolean autoCreate)
    {
        this.autoCreate = autoCreate;
    }

    public void setDefaultChannel(int defaultChannel)
    {
        this.defaultChannel = defaultChannel;
    }

    public void setCriticalByDef(boolean criticalByDef)
    {
        this.criticalByDef = criticalByDef;
    }
}
