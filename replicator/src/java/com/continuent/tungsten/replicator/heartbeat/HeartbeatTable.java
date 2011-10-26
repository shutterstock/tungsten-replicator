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

package com.continuent.tungsten.replicator.heartbeat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;

/**
 * Provides a definition for a heartbeat table, which measures latency between
 * master and slave. The heartbeat table is created with a single row that is
 * then update to track changes. This class provides methods to update the
 * table.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HeartbeatTable
{
    private static Logger      logger        = Logger
                                                     .getLogger(HeartbeatTable.class);

    public static final String TABLE_NAME    = "heartbeat";
    private static final long  KEY           = 1;

    private static AtomicLong  saltValue     = new AtomicLong(0);

    private Table              hbTable;
    private Column             hbId;
    private Column             hbSeqno;
    private Column             hbEventId;
    private Column             hbSourceTstamp;
    private Column             hbTargetTstamp;
    private Column             hbLagMillis;
    private Column             hbSalt;
    private Column             hbName;

    String                     sourceTsQuery = null;

    private String             tableType;

    public HeartbeatTable(String schema, String tableType)
    {
        this.tableType = tableType;
        initialize(schema);
    }

    private void initialize(String schema)
    {
        hbTable = new Table(schema, TABLE_NAME);
        hbId = new Column("id", Types.BIGINT);
        hbSeqno = new Column("seqno", Types.BIGINT);
        hbEventId = new Column("eventid", Types.VARCHAR, 128);
        hbSourceTstamp = new Column("source_tstamp", Types.TIMESTAMP);
        hbTargetTstamp = new Column("target_tstamp", Types.TIMESTAMP);
        hbLagMillis = new Column("lag_millis", Types.BIGINT);
        hbSalt = new Column("salt", Types.BIGINT);
        hbName = new Column("name", Types.VARCHAR, 128);

        Key hbKey = new Key(Key.Primary);
        hbKey.AddColumn(hbId);

        hbTable.AddColumn(hbId);
        hbTable.AddColumn(hbSeqno);
        hbTable.AddColumn(hbEventId);
        hbTable.AddColumn(hbSourceTstamp);
        hbTable.AddColumn(hbTargetTstamp);
        hbTable.AddColumn(hbLagMillis);
        hbTable.AddColumn(hbSalt);
        hbTable.AddColumn(hbName);
        hbTable.AddKey(hbKey);

        sourceTsQuery = "SELECT source_tstamp from " + schema + "."
                + TABLE_NAME + " where id=" + KEY;
    }
    
    /**
     * Returns metadata used to create the underlying heartbeat table.
     */
    public Table getTable()
    {
        return hbTable;
    }

    /**
     * Set up the heartbeat table on the master.
     */
    public void initializeHeartbeatTable(Database database) throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Initializing heartbeat table");

        // Replace the table.
        database.createTable(this.hbTable, false, tableType);

        // Add an initial heartbeat value if needed
        ResultSet res = null;
        PreparedStatement hbRowCount=null;
        int rows = 0;

        try
        {
            hbRowCount = database
                    .prepareStatement("SELECT count(*) from " + this.hbTable.getSchema() + "."
                            + this.hbTable.getName());
            res = hbRowCount.executeQuery();
            if (res.next())
            {
                rows = res.getInt(1);
            }
        }
        finally
        {
            if (res != null)
            {
                try
                {
                    res.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (hbRowCount != null)
            {
                try
                {
                    hbRowCount.close();
                }
                catch (Exception e)
                {
                }
            }
        }

        if (rows == 0)
        {

            hbId.setValue(KEY);
            hbSourceTstamp.setValue(new Timestamp(System.currentTimeMillis()));
            hbSalt.setValue(saltValue.getAndIncrement());
            database.insert(hbTable);
        }
    }

    /**
     * Execute this call to start a named heartbeat on the master. The heartbeat
     * table update must be logged as we will expect to see it as a DBMSEvent.
     */
    public void startHeartbeat(Database database, String name)
            throws SQLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();
        Timestamp now = new Timestamp(System.currentTimeMillis());

        if (logger.isDebugEnabled())
            logger.debug("Processing master heartbeat update: name=" + name
                    + " time=" + now);

        hbId.setValue(KEY);
        whereClause.add(hbId);
        hbSourceTstamp.setValue(now);
        hbSalt.setValue(saltValue.getAndIncrement());
        hbName.setValue(name);
        values.add(hbSourceTstamp);
        values.add(hbSalt);
        values.add(hbName);

        database.update(hbTable, whereClause, values);
    }

    /**
     * Wrapper for startHeartbeat() call.
     */
    public void startHeartbeat(String url, String user, String password, String name)
            throws SQLException
    {
        Database db = null;
        try
        {
            db = DatabaseFactory.createDatabase(url, user, password);
            db.connect(true);
            startHeartbeat(db, name);
        }
        finally
        {
            db.close();
        }
    }

    /**
     * Execute this call to fill in heartbeat data on the slave. This call must
     * be invoked after a heartbeat event is applied.
     */
    public void completeHeartbeat(Database database, long seqno, String eventId)
            throws SQLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Processing slave heartbeat update");

        Statement st = null;
        ResultSet rs = null;
        Timestamp sts = new Timestamp(0);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();

        if (logger.isDebugEnabled())
            logger.debug("Processing slave heartbeat update: " + now);

        // Get the source timestamp.
        try
        {
            st = database.createStatement();
            rs = st.executeQuery(sourceTsQuery);
            if (rs.next())
                sts = rs.getTimestamp(1);
        }
        finally
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
            if (st != null)
            {
                try
                {
                    st.close();
                }
                catch (SQLException e)
                {
                }
            }
        }

        // Compute the difference between source and target.
        long lag_millis = now.getTime() - sts.getTime();

        // Update the heartbeat record with target time and difference.
        hbId.setValue(KEY);
        whereClause.add(hbId);

        hbSeqno.setValue(seqno);
        hbEventId.setValue(eventId);
        hbTargetTstamp.setValue(now);
        hbLagMillis.setValue(lag_millis);
        values.add(hbSeqno);
        values.add(hbEventId);
        values.add(hbTargetTstamp);
        values.add(hbLagMillis);

        database.update(hbTable, whereClause, values);
    }
}