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

package com.continuent.tungsten.replicator.thl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.GreenplumDatabase;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;

/**
 * Encapsulates definition and management of the trep_commit_seqno table, which
 * is used to ensure consistency between the current contents of the history
 * table and the actual database. Here are the rules for master and slave:
 * <p>
 * <ul>
 * <li>Master - Master must update trep_commit_seqno whenever an extracted event
 * is stored.</li>
 * <li>Slave - Slave must update trep_commit_seqno whenever an event is applied</li>
 * </ul>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class CommitSeqnoTable
{
    private static Logger      logger                   = Logger.getLogger(CommitSeqnoTable.class);

    public static final String TABLE_NAME               = "trep_commit_seqno";

    // Properties.
    private final String       schema;
    private final Database     database;
    private final boolean      syncNativeSlaveRequired;

    private Table              commitSeqnoTable;
    private Column             commitSeqnoTableTaskId;
    private Column             commitSeqnoTableSeqno;
    private Column             commitSeqnoTableFragno;
    private Column             commitSeqnoTableLastFrag;
    private Column             commitSeqnoTableSourceId;
    private Column             commitSeqnoTableEpochNumber;
    private Column             commitSeqnoTableEventId;
    private Column             commitSeqnoTableAppliedLatency;
    private Column             commitSeqnoTableExtractTimestamp;
    private Column             commitSeqnoTableUpdateTimestamp;
    private Column             commitSeqnoTableShardId;

    /* Greenplum specifics */
    private String             GREENPLUM_DISTRIBUTED_BY = "greenplumn_id";

    private PreparedStatement  commitSeqnoUpdate;
    private PreparedStatement  lastSeqnoQuery;

    private String             tableType;

    /**
     * Creates a new instance valid for life of the provided database
     * connection.
     */
    public CommitSeqnoTable(Database database, String schema, String tableType,
            boolean syncNativeSlaveRequired)
    {
        this.database = database;
        this.schema = schema;
        this.tableType = tableType;
        this.syncNativeSlaveRequired = syncNativeSlaveRequired;
    }

    /**
     * Prepares the instance for use. This must occur before use.
     */
    public void prepare(int taskId) throws SQLException
    {
        // Define schema.
        commitSeqnoTable = new Table(schema, TABLE_NAME);
        commitSeqnoTableTaskId = new Column("task_id", java.sql.Types.INTEGER);
        commitSeqnoTableSeqno = new Column("seqno", java.sql.Types.BIGINT);
        commitSeqnoTableFragno = new Column("fragno", Types.SMALLINT);
        commitSeqnoTableLastFrag = new Column("last_frag", Types.CHAR, 1);
        commitSeqnoTableSourceId = new Column("source_id", Types.VARCHAR, 128);
        commitSeqnoTableEpochNumber = new Column("epoch_number", Types.BIGINT);
        commitSeqnoTableEventId = new Column("eventid", Types.VARCHAR, 128);
        commitSeqnoTableAppliedLatency = new Column("applied_latency",
                Types.INTEGER);
        commitSeqnoTableUpdateTimestamp = new Column("update_timestamp",
                Types.TIMESTAMP);
        commitSeqnoTableShardId = new Column("shard_id", Types.VARCHAR, 128);
        commitSeqnoTableExtractTimestamp = new Column("extract_timestamp",
                Types.TIMESTAMP);

        commitSeqnoTable.AddColumn(commitSeqnoTableTaskId);
        commitSeqnoTable.AddColumn(commitSeqnoTableSeqno);
        commitSeqnoTable.AddColumn(commitSeqnoTableFragno);
        commitSeqnoTable.AddColumn(commitSeqnoTableLastFrag);
        commitSeqnoTable.AddColumn(commitSeqnoTableSourceId);
        commitSeqnoTable.AddColumn(commitSeqnoTableEpochNumber);
        commitSeqnoTable.AddColumn(commitSeqnoTableEventId);
        commitSeqnoTable.AddColumn(commitSeqnoTableAppliedLatency);
        commitSeqnoTable.AddColumn(commitSeqnoTableUpdateTimestamp);
        commitSeqnoTable.AddColumn(commitSeqnoTableShardId);
        commitSeqnoTable.AddColumn(commitSeqnoTableExtractTimestamp);

        if (database instanceof GreenplumDatabase)
        {
            // Add a serialized distribution column for the table.
            Column commitSeqnoTableGreenplumId = new Column(
                    GREENPLUM_DISTRIBUTED_BY, java.sql.Types.INTEGER);
            commitSeqnoTable.AddColumn(commitSeqnoTableGreenplumId);
        }

        Key pkey = new Key(Key.Primary);
        pkey.AddColumn(commitSeqnoTableTaskId);
        commitSeqnoTable.AddKey(pkey);

        // Prepare SQL.
        lastSeqnoQuery = database
                .prepareStatement("SELECT seqno, fragno, last_frag, source_id, epoch_number, eventid, shard_id, extract_timestamp from "
                        + schema + "." + TABLE_NAME + " WHERE task_id=?");

        commitSeqnoUpdate = database.prepareStatement("UPDATE "
                + commitSeqnoTable.getSchema() + "."
                + commitSeqnoTable.getName() + " SET "
                + commitSeqnoTableSeqno.getName() + "=?, "
                + commitSeqnoTableFragno.getName() + "=?, "
                + commitSeqnoTableLastFrag.getName() + "=?, "
                + commitSeqnoTableSourceId.getName() + "=?, "
                + commitSeqnoTableEpochNumber.getName() + "=?, "
                + commitSeqnoTableEventId.getName() + "=?, "
                + commitSeqnoTableAppliedLatency.getName() + "=?, "
                + commitSeqnoTableUpdateTimestamp.getName() + "=?, "
                + commitSeqnoTableShardId.getName() + "=?, "
                + commitSeqnoTableExtractTimestamp.getName() + "=? " + "WHERE "
                + commitSeqnoTableTaskId.getName() + "=?");

        // Create the table if it does not exist.
        if (logger.isDebugEnabled())
            logger.debug("Initializing " + TABLE_NAME + " table");

        database.createTable(commitSeqnoTable, false, tableType);

        if (database instanceof GreenplumDatabase)
        {
            // Specify distribution column for the table.
            ((GreenplumDatabase) database).setDistributedBy(schema,
                    commitSeqnoTable.getName(), GREENPLUM_DISTRIBUTED_BY);
        }

        // Check to see if we need to initialize data for this task ID.
        if (lastCommitSeqno(taskId) == null)
        {
            // Always initialize to default value.
            commitSeqnoTableTaskId.setValue(taskId);
            commitSeqnoTableSeqno.setValue(-1L);
            commitSeqnoTableEventId.setValue(null);
            commitSeqnoTableUpdateTimestamp.setValue(new Timestamp(System
                    .currentTimeMillis()));

            database.insert(commitSeqnoTable);

            // If there is a task 0 commit seqno, we propagate task 0 position
            // into succeeding tasks.
            ReplDBMSHeader task0CommitSeqno = lastCommitSeqno(0);
            if (task0CommitSeqno == null)
            {
                logger.info("Initializing trep_commit_seqno defaults for task: "
                        + taskId);
            }
            else
            {
                logger.info("Propagating trep_commit_seqno data from task 0 to task: "
                        + taskId);
                updateLastCommitSeqno(taskId, task0CommitSeqno, 0);
            }
        }
    }

    /**
     * Releases the instance. This must occur after last use.
     */
    public void release()
    {
        if (lastSeqnoQuery != null)
            close(lastSeqnoQuery);
        if (commitSeqnoUpdate != null)
            close(commitSeqnoUpdate);
    }

    /**
     * Fetches header data for last committed transaction or null if none such
     * can be found.
     */
    public ReplDBMSHeader lastCommitSeqno(int taskId) throws SQLException
    {
        ReplDBMSHeaderData header = null;
        ResultSet res = null;

        try
        {
            lastSeqnoQuery.setInt(1, taskId);
            res = lastSeqnoQuery.executeQuery();
            if (res.next())
            {
                header = headerFromResult(res);
            }
        }
        finally
        {
            close(res);
        }

        // Return whatever we found, including null value.
        return header;
    }

    /**
     * Reduces the trep_commit_seqno table to task 0 entry *provided* there is a
     * task 0 row and provide all rows are at the same sequence number. This
     * operation allows the table to convert to a different number of apply
     * threads.
     */
    public void reduceTasks() throws SQLException
    {
        boolean hasTask0 = false;
        boolean hasCommonSeqno = true;
        long commonSeqno = -1;
        PreparedStatement allSeqnosQuery = null;
        PreparedStatement deleteQuery = null;
        ResultSet rs = null;

        try
        {
            // Scan task positions.
            allSeqnosQuery = database
                    .prepareStatement("SELECT seqno, fragno, last_frag, source_id, epoch_number, eventid, shard_id, extract_timestamp, task_id from "
                            + schema + "." + TABLE_NAME);
            String lastEventId = null;
            rs = allSeqnosQuery.executeQuery();
            while (rs.next())
            {
                // Look for a common sequence number.
                ReplDBMSHeader header = headerFromResult(rs);
                if (commonSeqno == -1)
                    commonSeqno = header.getSeqno();
                else if (commonSeqno != header.getSeqno())
                    hasCommonSeqno = false;

                // Store the event ID. This is only used if we reduce, in which
                // case event IDs on all rows are the same.
                if (lastEventId == null)
                    lastEventId = rs.getString(6);

                // Check for task 0.
                int task_id = rs.getInt(9);
                if (task_id == 0)
                    hasTask0 = true;
            }

            // See if we can reduce the table to task 0.
            if (!hasTask0)
            {
                logger.warn("No task 0 present; cannot reduce task entries: "
                        + schema + "." + TABLE_NAME);
            }
            else if (!hasCommonSeqno)
            {
                logger.warn("Sequence numbers do not match; cannot reduce task entries: "
                        + schema + "." + TABLE_NAME);
            }
            else
            {
                // Reduce rows.
                deleteQuery = database.prepareStatement("DELETE FROM " + schema
                        + "." + TABLE_NAME + " WHERE task_id > 0");
                int reducedRows = deleteQuery.executeUpdate();
                logger.info("Reduced " + reducedRows + " task entries: "
                        + schema + "." + TABLE_NAME);

                // If appropriate, synchronize native replication.
                if (syncNativeSlaveRequired && lastEventId != null)
                {
                    if (database.supportsNativeSlaveSync())
                    {
                        logger.info("Synchronizing native slave replication to current event ID: "
                                + lastEventId);
                        database.syncNativeSlave(lastEventId);
                    }
                    else
                    {
                        logger.warn("Native slave synchronization required but DBMS implementation does not support it");
                    }
                }
            }
        }
        finally
        {
            close(rs);
            close(allSeqnosQuery);
            close(deleteQuery);
        }
    }

    /**
     * Updates the last commit seqno value.
     */
    public void updateLastCommitSeqno(int taskId, ReplDBMSHeader header,
            long appliedLatency) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Updating last committed event header: "
                    + header.getSeqno());

        commitSeqnoUpdate.setLong(1, header.getSeqno());
        commitSeqnoUpdate.setShort(2, header.getFragno());
        commitSeqnoUpdate.setBoolean(3, header.getLastFrag());
        commitSeqnoUpdate.setString(4, header.getSourceId());
        commitSeqnoUpdate.setLong(5, header.getEpochNumber());
        commitSeqnoUpdate.setString(6, header.getEventId());
        // Latency can go negative due to clock differences. Round up to 0.
        commitSeqnoUpdate.setLong(7, Math.abs(appliedLatency));
        new Timestamp(System.currentTimeMillis());
        commitSeqnoUpdate.setTimestamp(8,
                new Timestamp(System.currentTimeMillis()));
        commitSeqnoUpdate.setString(9, header.getShardId());
        commitSeqnoUpdate.setTimestamp(10, header.getExtractedTstamp());
        commitSeqnoUpdate.setInt(11, taskId);

        commitSeqnoUpdate.executeUpdate();
    }

    // Return a header from a trep_commit_seqno result.
    private ReplDBMSHeaderData headerFromResult(ResultSet rs)
            throws SQLException
    {
        long seqno = rs.getLong(1);
        short fragno = rs.getShort(2);
        boolean lastFrag = rs.getBoolean(3);
        String sourceId = rs.getString(4);
        long epochNumber = rs.getLong(5);
        String eventId = rs.getString(6);
        String shardId = rs.getString(7);
        Timestamp extractTimestamp = rs.getTimestamp(8);

        return new ReplDBMSHeaderData(seqno, fragno, lastFrag, sourceId,
                epochNumber, eventId, shardId, extractTimestamp);
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