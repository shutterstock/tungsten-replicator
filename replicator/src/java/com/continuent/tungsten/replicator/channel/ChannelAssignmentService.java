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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.channel;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.service.PipelineService;

/**
 * Provides a service interface to the shard-to-channel assignment table.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ChannelAssignmentService implements PipelineService
{
    private static Logger        logger      = Logger.getLogger(ChannelAssignmentService.class);
    private String               name;
    private String               user;
    private String               url;
    private String               password;
    private int                  channels;

    private Database             conn;
    private ShardChannelTable    channelTable;
    private Map<String, Integer> assignments = new HashMap<String, Integer>();
    private int                  maxChannel;
    private int                  nextChannel = 0;
    private int                  accessFailures;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Assign connection parameters. The URL may be necessary to ensure that
        // replicator schema is created.
        if (url == null)
        {
            url = context.getJdbcUrl(context.getReplicatorSchemaName());
        }
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Create the database connection.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to connect to database: "
                    + e.getMessage(), e);
        }

        // Create shard-channel table if it does not exist.
        channelTable = new ShardChannelTable(context.getReplicatorSchemaName());
        try
        {
            channelTable.initializeShardTable(conn);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to initialize shard-channel table", e);
        }

        // Load channel assignments.
        loadChannelAssignments();
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
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.Service#status()
     */
    public synchronized List<Map<String, String>> listChannelAssignments()
    {
        List<Map<String, String>> channels = null;
        try
        {
            channels = channelTable.list(conn);
        }
        catch (SQLException e)
        {
            accessFailures++;
            if (logger.isDebugEnabled())
                logger.debug("Channel table access failed", e);
            channels = new ArrayList<Map<String, String>>();
        }
        return channels;
    }

    /**
     * Inserts a shard/channel assignment.
     * 
     * @param shardId Shard name
     * @param channel Channel number
     * @throws ReplicatorException Thrown if there is an error accessing
     *             database
     */
    public synchronized void insertChannelAssignment(String shardId, int channel)
            throws ReplicatorException
    {
        try
        {
            channelTable.insert(conn, shardId, channel);
            assignments.put(shardId, channel);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to access channel assignment table; ensure it is defined",
                    e);
        }
    }

    /**
     * Looks up a channel assignment for a shard. This creates a new assignment
     * if required.
     * 
     * @param shardId Shard name
     * @return Integer channel number for shard
     * @throws ReplicatorException Thrown if there is an error accessing
     *             database
     */
    public synchronized Integer getChannelAssignment(String shardId)
            throws ReplicatorException
    {
        // See if we have a channel.
        Integer channel = assignments.get(shardId);

        // If not we need to create a brand new assignment.
        if (channel == null)
        {
            // Roll over partition number if necessary.
            if (nextChannel >= channels)
            {
                nextChannel = 0;
            }
            channel = nextChannel++;

            // Assign the new partition in the channel assignment
            // table.
            insertChannelAssignment(shardId, channel);
        }

        // Return the channel.
        return channel;
    }

    // Load current channel assignments from the database.
    private synchronized void loadChannelAssignments()
            throws ReplicatorException
    {
        try
        {
            List<Map<String, String>> rows = channelTable.list(conn);
            for (Map<String, String> assignment : rows)
            {
                String shardId = assignment.get(ShardChannelTable.SHARD_ID_COL);
                Integer channel = Integer.parseInt(assignment
                        .get(ShardChannelTable.CHANNEL_COL));
                assignments.put(shardId, channel);
            }
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(
                    "Unable to access shard assignment table; ensure it is defined",
                    e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.service.Service#status()
     */
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString("name", name);
        props.setLong("totalAssignments", assignments.size());
        props.setLong("maxChannel", maxChannel);
        props.setLong("accessFailures", accessFailures);
        return props;
    }
}