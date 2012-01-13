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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Encapsulates management of shard to channel assignments. Such assignments are
 * used to manage parallel apply with dynamic mapping of new shards to channels.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class ShardChannelTable
{
    private static Logger      logger       = Logger.getLogger(ShardChannelTable.class);

    public static final String TABLE_NAME   = "trep_shard_channel";

    public static final String SHARD_ID_COL = "shard_id";
    public static final String CHANNEL_COL  = "channel";

    private String             select       = "SELECT " + SHARD_ID_COL + ", "
                                                    + CHANNEL_COL + " FROM "
                                                    + TABLE_NAME + " ORDER BY "
                                                    + SHARD_ID_COL;

    private Table              channelTable;
    private Column             shardId;
    private Column             channel;

    /**
     * Create and initialize a new shard channel table.
     * 
     * @param schema
     */
    public ShardChannelTable(String schema)
    {
        initialize(schema);
    }

    /**
     * Initialize DBMS access structures.
     */
    private void initialize(String schema)
    {
        channelTable = new Table(schema, TABLE_NAME);
        shardId = new Column(SHARD_ID_COL, Types.VARCHAR, 128);
        channel = new Column(CHANNEL_COL, Types.INTEGER);

        Key shardKey = new Key(Key.Primary);
        shardKey.AddColumn(shardId);

        channelTable.AddColumn(shardId);
        channelTable.AddColumn(channel);
        channelTable.AddKey(shardKey);

        select = "SELECT " + SHARD_ID_COL + ", " + CHANNEL_COL + " FROM "
                + schema + "." + TABLE_NAME + " ORDER BY " + SHARD_ID_COL;
    }

    /**
     * Set up the channel table.
     */
    public void initializeShardTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing channel table");

        // Replace the table.
        database.createTable(this.channelTable, false);
    }

    /**
     * Insert a shard/channel assignment into the database.
     */
    public int insert(Database database, String shardName, int channelNumber)
            throws SQLException
    {
        shardId.setValue(shardName);
        channel.setValue(channelNumber);
        return database.insert(channelTable);
    }

    /**
     * Drop all channel definitions.
     */
    public int deleleAll(Database database) throws SQLException
    {
        return database.delete(channelTable, true);
    }

    /**
     * Return a list of currently known shard/channel assignments.
     */
    public List<Map<String, String>> list(Database conn) throws SQLException
    {
        ResultSet rs = null;
        Statement statement = conn.createStatement();
        List<Map<String, String>> shardToChannels = new ArrayList<Map<String, String>>();
        try
        {
            rs = statement.executeQuery(select);

            while (rs.next())
            {
                Map<String, String> shard = new HashMap<String, String>();

                shard.put(ShardChannelTable.SHARD_ID_COL,
                        rs.getString(ShardChannelTable.SHARD_ID_COL));
                shard.put(ShardChannelTable.CHANNEL_COL,
                        rs.getString(ShardChannelTable.CHANNEL_COL));
                shardToChannels.add(shard);
            }
        }
        finally
        {
            close(rs);
            close(statement);
        }
        return shardToChannels;
    }

    // Close a result set properly.
    private void close(ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException e)
            {
            }
        }
    }

    // Close a statement properly.
    private void close(Statement s)
    {
        if (s != null)
        {
            try
            {
                s.close();
            }
            catch (SQLException e)
            {
            }
        }
    }
}