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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.enterprise.replicator.shard;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Provides a definition for the shard table, which is a catalog of currently
 * known shards.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardTable
{
    private static Logger      logger     = Logger.getLogger(ShardTable.class);

    public static final String TABLE_NAME = "trep_shard";

    private Table              shardTable;
    private Column             shardId;
    private Column             shardState;

    private String             tableType;

    public ShardTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
    }

    private void initialize(String schema)
    {
        shardTable = new Table(schema, TABLE_NAME);
        shardId = new Column("id", Types.VARCHAR, 128);
        shardState = new Column("state", Types.VARCHAR, 12);

        Key shardKey = new Key(Key.Primary);
        shardKey.AddColumn(shardId);

        shardTable.AddColumn(shardId);
        shardTable.AddColumn(shardState);
        shardTable.AddKey(shardKey);
    }

    /**
     * Set up the heartbeat table on the master.
     */
    public void initializeShardTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing heartbeat table");

        // Replace the table.
        database.createTable(this.shardTable, false, tableType);
    }

    /**
     * Returns a list of schemas that can be turned into shards.
     */
    public List<String> getSchemas(Database database) throws SQLException
    {
        return database.getSchemas();
    }
}