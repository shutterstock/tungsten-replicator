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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.jmx.JmxManager;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardManager implements ShardManagerMBean
{
    private static Logger logger = Logger.getLogger(ShardManager.class);
    private String        serviceName;
    private String        url;
    private String        user;
    private String        password;
    private String        schema;
    private String        tableType;

    public ShardManager(String serviceName, String url, String user,
            String password, String tungstenSchema, String tungstenTableType)
    {
        this.serviceName = serviceName;
        this.url = url;
        this.user = user;
        this.password = password;
        this.schema = tungstenSchema;
        this.tableType = tungstenTableType;
    }

    public ShardManager(String serviceName, ReplicatorRuntime context)
    {
        this(serviceName,
                context.getJdbcUrl(context.getReplicatorSchemaName()), context
                        .getJdbcUser(), context.getJdbcPassword(), context
                        .getReplicatorSchemaName(), context
                        .getTungstenTableType());
    }

    @Override
    public boolean isAlive()
    {
        return true;
    }

    @Override
    public int insert(List<Map<String, String>> params) throws SQLException
    {
        int shardsCount = 0;
        if (logger.isDebugEnabled())
        {
            logger.debug("Inserting new shards for service " + serviceName);
        }

        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = (Map<String, String>) iterator
                        .next();
                try
                {
                    shardsCount += shardTable.insert(connection, new Shard(
                            shard));
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to insert new shard definition ("
                            + shard + ") : " + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }
        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    public int update(List<Map<String, String>> params) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Updating shards for service " + serviceName);

        int shardsCount = 0;
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = (Map<String, String>) iterator
                        .next();
                try
                {
                    shardsCount += shardTable.update(connection, new Shard(
                            shard));
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to update shard definitions (" + shard
                            + ") : " + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }

        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    public int delete(List<Map<String, String>> params) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Deleting shards for service " + serviceName);

        int shardsCount = 0;
        Database connection = null;

        ShardTable shardTable = new ShardTable(schema, tableType);
        try
        {
            connection = getConnection();
            for (Iterator<Map<String, String>> iterator = params.iterator(); iterator
                    .hasNext();)
            {
                Map<String, String> shard = (Map<String, String>) iterator
                        .next();
                String id = shard.get(ShardTable.SHARD_ID_COL);
                try
                {
                    shardsCount += shardTable.delete(connection, id);
                }
                catch (SQLException e)
                {
                    logger.warn("Failed to delete shard " + id + " : "
                            + e.getMessage());
                    if (logger.isDebugEnabled())
                        logger.debug("Full stack trace", e);
                }
            }
        }
        finally
        {
            connection.close();
        }
        return shardsCount;
    }

    @Override
    public int deleteAll() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Deleting all shards for service " + serviceName);
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            return shardTable.deleleAll(connection);
        }
        finally
        {
            if (connection != null)
                connection.close();
        }
    }

    @Override
    public List<Map<String, String>> list() throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Listing defined shards for service " + serviceName);
        Database connection = null;
        ShardTable shardTable = new ShardTable(schema, tableType);

        try
        {
            connection = getConnection();
            return shardTable.list(connection);
        }
        finally
        {
            if (connection != null)
                connection.close();
        }
    }

    private Database getConnection() throws SQLException
    {
        Database db = null;
        db = DatabaseFactory.createDatabase(url, user, password);
        db.connect(false);

        return db;
    }

    public void advertiseInternal()
    {
        if (logger.isDebugEnabled())
            logger.debug("Registering ShardManagerMBean for service "
                    + serviceName);
        JmxManager.registerMBean(this, ShardManager.class, serviceName, true);
    }

}
