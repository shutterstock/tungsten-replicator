/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.shard;

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
 * Provides a definition for the shard table, which is a catalog of currently
 * known shards.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardTable
{
    private static Logger       logger            = Logger.getLogger(ShardTable.class);

    public static final String  TABLE_NAME        = "trep_shard";

    public static final String  SHARD_ID_COL      = "name";
    public static final String  SHARD_CRIT_COL    = "critical";
    public static final String  SHARD_DISPO_COL   = "disposition";
    public static final String  SHARD_MASTER_COL  = "master";

    private static final String SELECT            = "SELECT " + SHARD_ID_COL
                                                          + ", "
                                                          + SHARD_MASTER_COL
                                                          + ", "
                                                          + SHARD_CRIT_COL
                                                          + " FROM "
                                                          + TABLE_NAME
                                                          + " ORDER BY "
                                                          + SHARD_MASTER_COL
                                                          + ", " + SHARD_ID_COL;

    private Table               shardTable;
    private Column              shardName;
    private Column              shardCritical;
    private Column              shardMaster;

    private String              tableType;

    public ShardTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
    }

    private void initialize(String schema)
    {
        shardTable = new Table(schema, TABLE_NAME);
        shardMaster = new Column(SHARD_MASTER_COL, Types.VARCHAR, 128);
        shardName = new Column(SHARD_ID_COL, Types.VARCHAR, 128);
        shardCritical = new Column(SHARD_CRIT_COL, Types.TINYINT, 1);

        Key shardKey = new Key(Key.Primary);
        shardKey.AddColumn(shardName);

        shardTable.AddColumn(shardName);
        shardTable.AddColumn(shardMaster);
        shardTable.AddColumn(shardCritical);
        shardTable.AddKey(shardKey);
    }

    /**
     * Set up the shard table.
     */
    public void initializeShardTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing shard table");

        // Replace the table.
        database.createTable(this.shardTable, false, tableType);
    }

    public int insert(Database database, Shard shard) throws SQLException
    {
        shardName.setValue(shard.getShardId());
        shardMaster.setValue(shard.getMaster());
        shardCritical.setValue(shard.isCritical());
        return database.insert(shardTable);
    }

    public int update(Database database, Shard shard) throws SQLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();

        shardName.setValue(shard.getShardId());
        whereClause.add(shardName);

        shardCritical.setValue(shard.isCritical());
        shardMaster.setValue(shard.getMaster());
        values.add(shardMaster);
        values.add(shardCritical);

        return database.update(shardTable, whereClause, values);
    }

    public int deleleAll(Database database) throws SQLException
    {
        return database.delete(shardTable, true);
    }

    public int delete(Database database, String id) throws SQLException
    {
        shardName.setValue(id);

        return database.delete(shardTable, false);
    }

    public List<Map<String, String>> list(Database conn) throws SQLException
    {
        ResultSet rs = null;
        Statement statement = conn.createStatement();
        List<Map<String, String>> shards = new ArrayList<Map<String, String>>();
        try
        {
            rs = statement.executeQuery(ShardTable.SELECT);

            while (rs.next())
            {
                Map<String, String> shard = new HashMap<String, String>();

                shard.put(ShardTable.SHARD_ID_COL,
                        rs.getString(ShardTable.SHARD_ID_COL));
                shard.put(ShardTable.SHARD_CRIT_COL, Boolean.toString(rs
                        .getBoolean(ShardTable.SHARD_CRIT_COL)));

                shard.put(ShardTable.SHARD_MASTER_COL,
                        rs.getString(ShardTable.SHARD_MASTER_COL));
                shards.add(shard);

            }
        }
        finally
        {
            statement.close();
        }
        return shards;
    }

}