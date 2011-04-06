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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Linas Virbalas, Stephane Giron
 */

package com.continuent.tungsten.replicator.thl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;

/**
 * This class defines a JdbcTHLDatabase
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class JdbcTHLDatabase
{
    static Logger             logger                    = Logger
                                                                .getLogger(JdbcTHLDatabase.class);

    private int               taskId                    = 0;
    private ReplicatorRuntime runtime                   = null;
    private String            metadataSchema            = null;
    private Database          conn                      = null;

    private Statement         statement                 = null;

    private Table             history                   = null;
    private Column            historySeqno              = null;
    private Column            historyFragno             = null;
    private Column            historyLastFrag           = null;
    private Column            historySourceId           = null;
    private Column            historyType               = null;
    private Column            historyEpochNumber;
    private Column            historySourceTstamp       = null;
    private Column            historyLocalEnqueueTstamp = null;
    private Column            historyProcessedTstamp    = null;
    private Column            historyStatus             = null;
    private Column            historyComment            = null;
    private Column            historyEventId            = null;
    private Column            historyEvent              = null;

    private static int        commentLength             = 128;

    private CommitSeqnoTable  commitSeqnoTable          = null;

    /**
     * Creates a new <code>JdbcTHLDatabase</code> object
     * 
     * @param driver JDBC driver specification
     * @throws THLException
     */
    public JdbcTHLDatabase(String driver) throws THLException
    {
        // Load driver if provided.
        if (driver != null)
        {
            try
            {
                Class.forName(driver);
            }
            catch (Exception e)
            {
                throw new THLException(e);
            }
        }
    }

    /**
     * Creates a new <code>JdbcTHLDatabase</code> object
     * 
     * @param runtime runtime
     * @param driver JDBC driver specification
     * @throws THLException
     */
    public JdbcTHLDatabase(ReplicatorRuntime runtime, String driver)
            throws THLException
    {
        this(driver);
        this.runtime = runtime;
    }

    /* Create table descriptions */
    private void prepareTables()
    {
        // Create the history table definition and try to create the table.
        history = new Table(this.metadataSchema, "history", true);
        historySeqno = new Column("seqno", Types.BIGINT);
        historyFragno = new Column("fragno", Types.SMALLINT);
        historyLastFrag = new Column("last_frag", Types.CHAR, 1);
        historySourceId = new Column("source_id", Types.VARCHAR, 128);
        historyType = new Column("type", Types.TINYINT);
        historyEpochNumber = new Column("epoch_number", Types.BIGINT);
        historySourceTstamp = new Column("source_tstamp", Types.TIMESTAMP);
        historyLocalEnqueueTstamp = new Column("local_enqueue_tstamp",
                Types.TIMESTAMP);
        historyProcessedTstamp = new Column("processed_tstamp", Types.TIMESTAMP);
        historyStatus = new Column("status", Types.TINYINT);
        /* "comment" is an illegal column number in oracle */
        historyComment = new Column("comments", Types.VARCHAR, commentLength);
        historyEventId = new Column("eventid", Types.VARCHAR, 128);
        historyEvent = new Column("event", Types.BLOB);
        Key historyPrimary = new Key(Key.Primary);
        Key historySecondary = new Key(Key.NonUnique);

        /* populate history table with columns */
        historyPrimary.AddColumn(historySeqno);
        historyPrimary.AddColumn(historyFragno);
        historySecondary.AddColumn(historyEventId);
        history.AddColumn(historySeqno);
        history.AddColumn(historyFragno);
        history.AddColumn(historyLastFrag);
        history.AddColumn(historySourceId);
        history.AddColumn(historyType);
        history.AddColumn(historyEpochNumber);
        history.AddColumn(historySourceTstamp);
        history.AddColumn(historyLocalEnqueueTstamp);
        history.AddColumn(historyProcessedTstamp);
        history.AddColumn(historyStatus);
        history.AddColumn(historyComment);
        history.AddColumn(historyEventId);
        history.AddColumn(historyEvent);
        history.AddKey(historyPrimary);
        history.AddKey(historySecondary);
    }

    /* Create tables according to table descriptions */
    private void createTables() throws SQLException
    {
        // Create history table.
        conn.createTable(history, false, runtime.getTungstenTableType());

        // Create commit seqno table.
        commitSeqnoTable = new CommitSeqnoTable(conn, metadataSchema, runtime
                .getTungstenTableType());
        commitSeqnoTable.prepare(taskId);

        // Create consistency table
        Table consistency = ConsistencyTable
                .getConsistencyTableDefinition(metadataSchema);
        conn.createTable(consistency, false, runtime.getTungstenTableType());

        // Create heartbeat table.
        HeartbeatTable heartbeatTable = new HeartbeatTable(metadataSchema,
                runtime.getTungstenTableType());
        heartbeatTable.initializeHeartbeatTable(conn);
    }

    /* Helper method to read ReplDBMSEvent from byte[] array */
    private ReplEvent eventFromBytes(byte[] bytes) throws THLException
    {
        ReplEvent retval;
        try
        {
            ByteArrayInputStream baib = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(baib);
            retval = (ReplEvent) ois.readObject();
            return retval;
        }
        catch (Exception e)
        {
            throw new THLException(e);
        }
    }

    /* Helper to parse result set */
    private THLEvent parseResultSet(ResultSet res) throws THLException,
            SQLException
    {
        long seqno = res.getLong(1);
        short fragno = res.getShort(2);
        boolean lastFrag = res.getBoolean(3);
        String sourceId = res.getString(4);
        short type = res.getShort(5);
        long epochNumber = res.getLong(6);
        Timestamp localEnqueueTstamp = res.getTimestamp(7);
        Timestamp sourceTstamp = res.getTimestamp(8);
        Timestamp processedTstamp = res.getTimestamp(9);
        short status = res.getShort(10);
        String error = res.getString(11);
        String eventId = res.getString(12);
        // Blob eventBlob = res.getBlob(13);
        byte[] eventBytes = conn.getBlobAsBytes(res, 13);

        return new THLEvent(seqno, fragno, lastFrag, sourceId, type,
                epochNumber, localEnqueueTstamp, sourceTstamp, processedTstamp,
                status, error, eventId, eventFromBytes(eventBytes));
    }

    /**
     * Find THLEvent from storage.
     * 
     * @param seqno Sequence number of the event
     * @return THLEvent or null if event was not found
     * @throws THLException
     */
    public THLEvent find(long seqno) throws THLException
    {
        ResultSet res = null;
        try
        {
            String query = "SELECT * FROM " + metadataSchema
                    + ".history WHERE seqno='" + seqno + "'";
            res = statement.executeQuery(query);
            if (res.next())
            {
                return parseResultSet(res);
            }
            return null;
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Generates SQL WHERE clause specifying the given number range.
     * 
     * @param field Field name to query for.
     * @param low The beginning of the interval (inclusive).
     * @param high The end of the interval (inclusive).
     * @param alwaysWhere true IFF caller wishes to start a WHERE clause even if
     *            low and high are null
     * @return A WHERE clause statement preceding with an emtpy space. Empty
     *         string if low and high are nulls. Eg. " WHERE x >= 3 AND x <= 5".
     */
    private String genSQLWhereIntervalClause(String field, Long low, Long high,
            boolean alwaysWhere)
    {
        String query = "";
        if (low != null || high != null || alwaysWhere)
            query += " WHERE";
        if (low != null)
            query += " " + field + ">='" + low + "'";
        if (low != null && high != null)
            query += " AND";
        if (high != null)
            query += " " + field + "<='" + high + "'";
        return query;
    }

    /**
     * Query for multiple THLEvent objects from the storage by a given sequence
     * number range.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @return ArrayList containing events in the ordered by seqno.
     * @throws THLException
     */
    public ArrayList<THLEvent> find(Long low, Long high) throws THLException
    {
        ResultSet res = null;
        try
        {
            String query = "SELECT * FROM " + metadataSchema + ".history";
            query += genSQLWhereIntervalClause("seqno", low, high, false);
            query += " order by seqno";
            res = statement.executeQuery(query);
            ArrayList<THLEvent> events = new ArrayList<THLEvent>();
            while (res.next())
            {
                // TODO: add support for fragmented events when they will be
                // implemented.
                THLEvent thlEvent = parseResultSet(res);
                events.add(thlEvent);
            }
            return events;
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * parseInterval takes strings of the form "<scalar><unitcode>
     * <scalar><unit> ... " and parses them into the number of seconds
     * represented by the interval. Current units are seconds ("s"), minutes
     * ("m"), hours ("h"), and days ("d"). Examples are "3s" -> 3 seconds "1m"
     * -> 60 seconds "1m 3s" -> 63 seconds
     * 
     * @param s : String to be parsed
     * @return : number of seconds represented by interval
     * @throws THLException
     */
    private int parseInterval(String s) throws THLException
    {
        int retval = 0;
        int currentValue = 0;
        int addValue = 0;

        for (char c : s.toCharArray())
        {
            switch (c)
            {
                case '9' :
                    addValue++;
                case '8' :
                    addValue++;
                case '7' :
                    addValue++;
                case '6' :
                    addValue++;
                case '5' :
                    addValue++;
                case '4' :
                    addValue++;
                case '3' :
                    addValue++;
                case '2' :
                    addValue++;
                case '1' :
                    addValue++;
                case '0' :
                    currentValue *= 10;
                    currentValue += addValue;
                    addValue = 0;
                    break;
                case ' ' :
                    break;
                case 's' :
                case 'S' : /* seconds */
                    retval += currentValue;
                    currentValue = 0;
                    break;
                case 'm' :
                case 'M' : /* minutes */
                    retval += currentValue * 60;
                    currentValue = 0;
                    break;
                case 'h' :
                case 'H' : /* hours */
                    retval += currentValue * 60 * 60;
                    currentValue = 0;
                    break;
                case 'd' :
                case 'D' : /* days */
                    retval += currentValue * 60 * 60 * 24;
                    currentValue = 0;
                    break;
                default :
                    throw new THLException("Illegal value for age " + s
                            + ". Acceptable formats are s, m, h, and d");
            }
        }
        if (currentValue != 0)
            throw new THLException("Missing units indicator on age \"" + s
                    + "\"");
        return retval;
    }

    public int delete(Long low, Long high) throws THLException
    {
        return delete(low, high, null);
    }

    /**
     * Delete THL events from the history table by a given sequence number
     * range. Warning: deletion cannot be undone.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @param age If not null, an interval from the current time that no records
     *            are to be purged from
     * @throws THLException
     * @return Count of deleted rows.
     */
    public int delete(Long low, Long high, String age) throws THLException
    {
        Long startingSeqNo = null;
        Long stoppingSeqNo = null;
        PreparedStatement prepareStatement = null;
        int rowsProcessed = 0;
        int errorCount = 0;
        int batchSize = 1000; // attempt to purge 1000 rows at a time.
        int notificationInterval = 10000;
        // display progress every 10,000 rows.

        int nextNotification = notificationInterval;
        int secondsBefore = 0;
        Timestamp now = null;
        boolean debug = false;
        boolean ageLimited = false;

        // first find maximum so we do not delete the last row.
        String maxQuery = "SELECT MAX(seqno) FROM " + metadataSchema
                + ".history";
        long maxSeqNo = execLongQuery(maxQuery);

        // if age limited scan, determine now() upfront.
        if (age != null)
        {
            ageLimited = true;
            secondsBefore = parseInterval(age);
            String nowQuery = "SELECT " + conn.getNowFunction();
            now = execTimestampQuery(nowQuery);
            if (debug)
                System.out.println("now = " + now);
        }
        else
        {
            secondsBefore = 0;
        }

        // Determine high stopping sequence number
        if (high == null)
            stoppingSeqNo = maxSeqNo - 1;
        else
            stoppingSeqNo = high;
        if (stoppingSeqNo > maxSeqNo - 1)
            stoppingSeqNo = maxSeqNo - 1;

        // Determine low starting sequence number.
        String minQuery = "SELECT MIN(seqno) FROM " + metadataSchema
                + ".history";
        long minSeqNo = execLongQuery(minQuery);
        if (low == null)
            startingSeqNo = minSeqNo;
        else
            startingSeqNo = low;

        // prepare delete statement locking in value of "now()" for duration of
        // the run.
        String deleteQuery;
        if (!ageLimited)
            deleteQuery = "DELETE FROM " + metadataSchema
                    + ".history WHERE seqno >= ? and seqno < ?";
        else
            deleteQuery = "DELETE FROM " + metadataSchema
                    + ".history WHERE seqno >= ? and seqno < ? and "
                    + conn.getTimeDiff(null, "processed_tstamp") + " > "
                    + secondsBefore;

        if (debug)
            System.out.println("Delete query = " + deleteQuery);

        try
        {
            conn.setAutoCommit(false); // to allow for batch commit
            prepareStatement = conn.prepareStatement(deleteQuery);
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }

        // Delete all rows between low and high
        Long highSeqNo = 0L;
        if (debug)
            System.out.println("Start seqno = " + startingSeqNo);
        if (debug)
            System.out.println("Stop  seqno = " + stoppingSeqNo);
        for (Long lowSeqNo = startingSeqNo; lowSeqNo < stoppingSeqNo; lowSeqNo = highSeqNo)
        {
            highSeqNo = lowSeqNo + batchSize;
            if (highSeqNo > stoppingSeqNo)
                highSeqNo = stoppingSeqNo;
            try
            {
                prepareStatement.setLong(1, lowSeqNo);
                prepareStatement.setLong(2, highSeqNo);
                if (ageLimited)
                    prepareStatement.setTimestamp(3, now);
                Long expectedRows = highSeqNo - lowSeqNo;
                int rows = prepareStatement.executeUpdate();
                conn.commit();
                rowsProcessed += rows;
                if (rowsProcessed > nextNotification)
                {
                    System.out
                            .println("Deleted " + nextNotification + " rows.");
                    nextNotification += notificationInterval;
                }

                if (debug)
                    System.out.println("Batch deleted " + rows);
                if (rows != expectedRows && ageLimited)
                {
                    System.out.println("Reached end of age based purge.");
                    break;
                }
            }
            catch (SQLException e)
            {
                if (debug)
                    System.out.println("Skipping error = " + e);
                // Ignore SQL exceptions
                errorCount++;
            }
        }
        try
        {
            // System.out.println("Commit: wrap up");
            conn.commit();
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            try
            {
                if (prepareStatement != null)
                    prepareStatement.close();
            }
            catch (SQLException e)
            {
            }
        }
        if (debug)
            System.out.println("Rows processed = " + rowsProcessed);
        if (debug)
            System.out.println("Errors         = " + errorCount);
        return rowsProcessed;
    }

    /**
     * Find THLEvent fragment from storage.
     * 
     * @param seqno Sequence number of the event
     * @param fragno Fragmen number of the event
     * @return THLEvent or null if event was not found
     * @throws THLException
     */
    public THLEvent find(long seqno, short fragno) throws THLException
    {
        ResultSet res = null;
        try
        {
            String query = "SELECT * FROM " + metadataSchema
                    + ".history WHERE seqno='" + seqno + "'" + " AND fragno='"
                    + fragno + "'";
            res = statement.executeQuery(query);
            if (res.next())
            {
                return parseResultSet(res);
            }
            return null;
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Find event identifier corresponding to sequence number.
     * 
     * @param seqno Sequence number of the event
     * @return Event identifier or null if not found
     * @throws THLException
     */
    public String getEventId(long seqno, String sourceId) throws THLException
    {
        String query = "SELECT eventId FROM " + metadataSchema
                + ".history WHERE seqno='" + seqno + "'" + " AND fragno='0'";
        if (sourceId != null)
        {
            query = query + " AND source_id='" + sourceId + "'";
        }
        return execStringQuery(query);
    }

    /**
     * Gets the max event identifier (actually, the identifier of the event
     * which has the max sequence number and an event identifier not null)
     * 
     * @param sourceId The source id if we want to ensure it matches the source
     *            id on the last event id
     * @return the max event identifier
     * @throws THLException if an error occurs
     */
    public String getMaxEventId(String sourceId) throws THLException
    {
        return getEventId(getMaxSeqnoWithNonNullEvent(), sourceId);
    }

    /**
     * Get greatest sequence number in history.
     * 
     * @return Greatest sequence number or -1 if history was empty
     * @throws THLException
     */
    public long getMaxSeqno() throws THLException
    {
        String query = "SELECT MAX(seqno) FROM " + metadataSchema + ".history";
        return execLongQuery(query);
    }

    /**
     * Get greatest sequence number in history with a non-null event ID. This is
     * used to determine the restart point in the database log.
     * 
     * @return Greatest sequence number or -1 if history was empty or did not
     *         contain events with valid event identifier.
     * @throws THLException
     */
    public long getMaxSeqnoWithNonNullEvent() throws THLException
    {
        // Find the maximum event ID. If there is nothing, we return.
        String eventIdQuery = "SELECT MAX(eventId) FROM " + metadataSchema
                + ".history";
        String maxEventId = execStringQuery(eventIdQuery);
        if (maxEventId == null)
            return -1;

        // Now look up the corresponding sequence number. Pick the highest
        // value, just in case.
        String seqnoQuery = "SELECT MAX(seqno) FROM " + metadataSchema
                + ".history WHERE eventID='" + maxEventId + "'";
        long seqno = execLongQuery(seqnoQuery);
        return seqno;
    }

    /**
     * Get sequence number of the last event which has been processed. This is
     * marked by the sequence number in trep_commit_seqno.
     * 
     * @return Sequence number of the last event which has been processed
     * @throws THLException
     */
    public long getMaxCompletedSeqno() throws THLException
    {
        String query = "SELECT seqno FROM " + metadataSchema
                + ".trep_commit_seqno";
        long seqno = execLongQuery(query);
        if (seqno > -1)
            return seqno;

        // TREP-316 - For pre-1.0.3 data we still need the following query.
        // This is a table scan but is required if the commit sequence
        // number has not been coordinated with the history table on the
        // master.
        query = "SELECT MAX(seqno) FROM " + metadataSchema
                + ".history WHERE status='" + THLEvent.COMPLETED
                + "' OR status='" + THLEvent.SKIPPED + "'";
        return execLongQuery(query);
    }

    /**
     * Get smallest sequence number in history.
     * 
     * @return Smallest sequence number of -1 if history was empty.
     * @throws THLException
     */
    public long getMinSeqno() throws THLException
    {
        String query = "SELECT MIN(seqno) FROM " + metadataSchema + ".history";
        return execLongQuery(query);
    }

    public long[] getMinMaxSeqno() throws THLException
    {
        ResultSet res = null;
        try
        {
            String query = "SELECT MIN(seqno), MAX(seqno) FROM "
                    + metadataSchema + ".history";
            res = statement.executeQuery(query);
            long[] ret = new long[]{-1, -1};
            if (res.next())
            {
                ret[0] = res.getLong(1);
                if (res.wasNull())
                {
                    ret[0] = -1;
                }
                else
                {
                    ret[1] = res.getLong(2);
                }
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Get count of events in the history.
     * 
     * @return Count of events.
     * @throws THLException
     */
    public long getEventCount() throws THLException
    {
        return getEventCount(null, null);
    }

    /**
     * Get count of events in the history belonging to a specified seqno range.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @return Count of events.
     * @throws THLException
     */
    public long getEventCount(Long low, Long high) throws THLException
    {
        ResultSet res = null;
        try
        {
            String query = "SELECT COUNT(seqno) FROM " + metadataSchema
                    + ".history";
            query += genSQLWhereIntervalClause("seqno", low, high, false);
            res = statement.executeQuery(query);
            long ret = -1;
            if (res.next())
            {
                ret = res.getLong(1);
                if (res.wasNull())
                    ret = -1;
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Store given THLEvent. For masters, we support synchronizing the commit
     * sequence number, which is required to ensure failover works.
     * 
     * @param event Event to be stored.
     * @param syncCommitSeqno If true, synchronize the commit sequence number
     * @throws THLException
     */
    public void store(THLEvent event, boolean syncCommitSeqno)
            throws THLException
    {
        try
        {
            long metricID = 0L;
            ReplEvent revent = event.getReplEvent();
            ByteArrayOutputStream baob = new ByteArrayOutputStream(); // NEW
            ObjectOutputStream oob = new ObjectOutputStream(baob); // NEW
            if (runtime != null && runtime.getMonitor().getDetailEnabled())
                metricID = runtime.getMonitor().startCPUEvent(
                        ReplicatorMonitor.CPU_DB_SERIAL);
            oob.writeObject(revent); // NEW
            byte[] barr = baob.toByteArray(); // NEW
            InputStream is = new ByteArrayInputStream(barr); // NEW
            if (runtime != null && runtime.getMonitor().getDetailEnabled())
                runtime.getMonitor().stopCPUEvent(
                        ReplicatorMonitor.CPU_DB_SERIAL, metricID);

            historySeqno.setValue(event.getSeqno()); // NEW
            historyFragno.setValue(event.getFragno());
            historyLastFrag.setValue(event.getLastFrag());
            historySourceId.setValue(truncate(event.getSourceId(),
                    historySourceId.getLength()));
            historyType.setValue(event.getType());
            historyEpochNumber.setValue(event.getEpochNumber());
            Timestamp now = new Timestamp(System.currentTimeMillis());
            historyLocalEnqueueTstamp.setValue(now);
            historySourceTstamp.setValue(event.getSourceTstamp());
            historyProcessedTstamp.setValue(now);
            historyStatus.setValue(event.getStatus());
            historyComment.setValue(null);
            historyEventId.setValue(event.getEventId()); // NEW
            historyEvent.setValue(is, barr.length); // NEW

            if (event.getFragno() == 0)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Starting new transaction");
                conn.setAutoCommit(false);
            }

            if (runtime != null && runtime.getMonitor().getDetailEnabled())
                metricID = runtime.getMonitor().startCPUEvent(
                        ReplicatorMonitor.CPU_INSERTTHL);
            // Store the event sequence number.
            conn.insert(history); // NEW
            // Master must also synchronize commit seqno.
            if (syncCommitSeqno)
            {
                updateCommitSeqnoTable(event);
            }
            if (runtime != null && runtime.getMonitor().getDetailEnabled())
                runtime.getMonitor().stopCPUEvent(
                        ReplicatorMonitor.CPU_INSERTTHL, metricID);

            if (event.getLastFrag())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Committing");
                conn.commit(); // NEW
                conn.setAutoCommit(true); // NEW
            }
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
        catch (java.io.IOException e)
        {
            throw new THLException(e);
        }
    }

    /**
     * TODO: updateCommitSeqnoTable definition.
     * 
     * @param event
     * @throws SQLException
     */
    public void updateCommitSeqnoTable(THLEvent event) throws SQLException
    {
        // Recreate header data.
        ReplDBMSHeaderData header = new ReplDBMSHeaderData(event.getSeqno(),
                event.getFragno(), event.getLastFrag(), event.getSourceId(),
                event.getEpochNumber(), event.getEventId());
        long applyLatency = (System.currentTimeMillis() - event
                .getSourceTstamp().getTime()) / 1000;
        commitSeqnoTable.updateLastCommitSeqno(taskId, header, applyLatency);
    }

    /**
     * Change status for given THLEvent.
     * 
     * @param seqno Sequence number of the event
     * @param fragno Fragment number of the event
     * @param status New status for the event
     * @param msg Comment message
     * @throws THLException
     */
    public void setStatus(long seqno, short fragno, short status, String msg)
            throws THLException
    {
        ArrayList<Column> whereClause = new ArrayList<Column>();
        ArrayList<Column> values = new ArrayList<Column>();
        try
        {
            historySeqno.setValue(seqno);
            historyFragno.setValue(fragno);
            whereClause.add(historySeqno);
            whereClause.add(historyFragno);
            historyStatus.setValue(status);
            values.add(historyStatus);
            historyProcessedTstamp.setValue(new Timestamp(System
                    .currentTimeMillis()));
            values.add(historyProcessedTstamp);
            if (msg != null)
            {
                historyComment.setValue(truncate(msg, commentLength));
                values.add(historyComment);
            }
            conn.update(history, whereClause, values);
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
    }

    public void connect(String url, String user, String password,
            String metadataSchema) throws THLException
    {
        connect(url, user, password, metadataSchema, null);
    }
    
    /**
     * Connect to database.
     * 
     * @param url Database url
     * @param user Database user name
     * @param password Database user password
     * @throws THLException
     */
    public void connect(String url, String user, String password,
            String metadataSchema, String vendor) throws THLException
    {
        this.metadataSchema = metadataSchema;
        // Prepare table descriptions
        prepareTables();
        // Create the database handle
        try
        {
            // Log updates for a remote data service.
            conn = DatabaseFactory.createDatabase(url, user, password, vendor);
            conn.connect(runtime.isRemoteService());

            statement = conn.createStatement();
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
    }

    /**
     * Prepare THLDatabase schema.
     * 
     * @throws THLException
     */
    public void prepareSchema() throws THLException
    {
        try
        {
            // Set default schema if supported.
            if (conn.supportsUseDefaultSchema() && metadataSchema != null)
            {
                // I believe createSchema() should take an argument which
                // essentially supports
                // create database <foo> IF NOT EXISTS. No reason to create the
                // schema
                // if it already exists and no reason to raise an error if it
                // does.
                // perhaps that IS the current meaning of "createSchema()" but
                // if that is
                // I did not see it documented to not raise and error if the
                // schema already
                // exists.
                if (conn.supportsCreateDropSchema())
                    conn.createSchema(metadataSchema);
                conn.useDefaultSchema(metadataSchema);
            }
            // Create tables, allowing schema changes to be logged if requested.
            if (conn.supportsControlSessionLevelLogging()
                    && runtime.logReplicatorUpdates())
            {
                logger.info("Logging schema creation");
                conn.controlSessionLevelLogging(false);
            }
            createTables();
            if (conn.supportsControlSessionLevelLogging())
                conn.controlSessionLevelLogging(true);
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
    }

    /**
     * Close database connection.
     */
    public void close()
    {
        // Reduce tasks in task table if possible. 
        try
        {
            commitSeqnoTable.reduceTasks();
        }
        catch (SQLException e)
        {
            logger.warn("Unable to reduce tasks information", e);
        }

        // Clean up JDBC connection. 
        statement = null;
        conn.close();
        conn = null;
    }

    public void updateSuccessStatus(ArrayList<THLEventStatus> succeededEvents,
            ArrayList<THLEventStatus> skippedEvents) throws THLException
    {
        Statement stmt = null;
        PreparedStatement pstmt = null;
        try
        {
            conn.setAutoCommit(false);

            // Update success, if any
            if (succeededEvents != null && succeededEvents.size() > 0)
            {
                stmt = conn.createStatement();
                String seqnoList = buildCommaSeparatedList(succeededEvents);
                stmt.executeUpdate("UPDATE " + history + " SET status = "
                        + THLEvent.COMPLETED + ", processed_tstamp = "
                        + conn.getNowFunction() + " WHERE seqno in "
                        + seqnoList);
            }

            // Update skipped, if any
            if (skippedEvents != null && skippedEvents.size() > 0)
            {
                pstmt = conn.prepareStatement("UPDATE " + history
                        + " SET status = ?, comments = ?,"
                        + " processed_tstamp = ? WHERE seqno = ?");
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (THLEventStatus event : skippedEvents)
                {
                    pstmt.setShort(1, THLEvent.SKIPPED);
                    pstmt.setString(2, truncate(event.getException() != null
                            ? event.getException().getMessage()
                            : "Unknown event failure", commentLength));
                    pstmt.setTimestamp(3, now);
                    pstmt.setLong(4, event.getSeqno());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                pstmt.close();
            }

            conn.commit();
        }
        catch (SQLException e)
        {
            THLException exception = new THLException(
                    "Failed to update events status");
            exception.initCause(e);
            try
            {
                conn.rollback();
            }
            catch (SQLException e1)
            {
                THLException exception2 = new THLException(
                        "Failed to rollback after failure while updating events status");
                e1.initCause(exception);
                exception2.initCause(e1);
                exception = exception2;
            }
            throw exception;
        }
        finally
        {
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException ignore)
                {
                }
            }
            if (pstmt != null)
            {
                try
                {
                    pstmt.close();
                }
                catch (SQLException ignore)
                {
                }
            }
            try
            {
                conn.setAutoCommit(true);
            }
            catch (SQLException ignore)
            {
            }
        }
    }

    public void updateFailedStatus(THLEventStatus failedEvent,
            ArrayList<THLEventStatus> events) throws THLException
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        Statement stmt = null;
        PreparedStatement pstmt = null;

        try
        {
            conn.setAutoCommit(false);

            if (events != null && events.size() > 0)
            {
                // 1. Mark all previously succeeded events as Failed, if any
                String seqnoList = buildCommaSeparatedList(events);

                stmt = conn.createStatement();

                stmt
                        .executeUpdate("UPDATE history SET status = "
                                + THLEvent.FAILED
                                + ", comments = 'Event was rollbacked due to failure while processing event#"
                                + failedEvent.getSeqno() + "'"
                                + ", processed_tstamp = "
                                + conn.getNowFunction() + " WHERE seqno in "
                                + seqnoList);

            }
            // 2. Mark the failed event
            pstmt = conn.prepareStatement("UPDATE history SET status = ?"
                    + ", comments = ?" + ", processed_tstamp = ?"
                    + " WHERE seqno = ?");
            pstmt.setShort(1, THLEvent.FAILED);
            pstmt.setString(2, truncate(failedEvent.getException() != null
                    ? failedEvent.getException().getMessage()
                    : "Unknown failure", commentLength));
            pstmt.setTimestamp(3, now);
            pstmt.setLong(4, failedEvent.getSeqno());
            pstmt.executeUpdate();

            // and commit transaction
            conn.commit();
        }
        catch (SQLException e)
        {
            THLException exception = new THLException(
                    "Failed to update events status");
            exception.initCause(e);
            try
            {
                conn.rollback();
            }
            catch (SQLException e1)
            {
                THLException exception2 = new THLException(
                        "Failed to rollback after failure while updating events status");
                e1.initCause(exception);
                exception2.initCause(e1);
                exception = exception2;
            }
            throw exception;
        }
        finally
        {
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException ignore)
                {
                }
            }

            if (pstmt != null)
            {
                try
                {
                    pstmt.close();
                }
                catch (SQLException ignore)
                {
                }
            }
            try
            {
                conn.setAutoCommit(true);
            }
            catch (SQLException ignore)
            {
            }
        }

    }

    private String buildCommaSeparatedList(ArrayList<THLEventStatus> events)
    {
        StringBuffer seqnoList = new StringBuffer("");
        if (events != null && !events.isEmpty())
        {
            seqnoList.append('(');
            for (THLEventStatus element : events)
            {
                if (seqnoList.length() > 1)
                    seqnoList.append(',');
                seqnoList.append(element.getSeqno());
            }
            seqnoList.append(')');
        }
        return seqnoList.toString();
    }

    // Utility routine to truncate string lengths.
    private String truncate(String string, long len)
    {
        if (string == null)
            return "";
        if (string.length() <= len)
            return string;
        else
            return string.substring(0, (int) (len - 1));
    }

    /**
     * Utility routine to execute a SQL query that returns a single long value.
     * 
     * @param query SQL query to execute
     * @return long value or -1 if null
     * @throws THLException thrown if there is a SQL exception
     */
    private long execLongQuery(String query) throws THLException
    {
        ResultSet res = null;
        try
        {
            res = statement.executeQuery(query);
            long ret = -1;
            if (res.next())
            {
                ret = res.getLong(1);
                if (res.wasNull())
                    ret = -1;
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException("SQL query failed: " + query, e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Utility routine to execute a SQL query that returns a single String
     * value.
     * 
     * @param query SQL query to execute
     * @return Sting value or -null
     * @throws THLException thrown if there is a SQL exception
     */
    private String execStringQuery(String query) throws THLException
    {
        ResultSet res = null;
        try
        {
            res = statement.executeQuery(query);
            String ret = null;
            if (res.next())
            {
                ret = res.getString(1);
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException("SQL query failed: " + query, e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Utility routine to execute a SQL query that returns a single Date value.
     * 
     * @param query SQL query to execute
     * @return Sting value or -null
     * @throws THLException thrown if there is a SQL exception
     */
    private Timestamp execTimestampQuery(String query) throws THLException
    {
        ResultSet res = null;
        try
        {
            res = statement.executeQuery(query);
            Timestamp ret = null;
            if (res.next())
            {
                ret = res.getTimestamp(1);
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException("SQL query failed: " + query, e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    public short getMaxFragno(long seqno) throws THLException
    {
        String query = "SELECT MAX(fragno) FROM " + metadataSchema
                + ".history where seqno='" + seqno + "'";

        ResultSet res = null;
        try
        {
            res = statement.executeQuery(query);
            short ret = -1;
            if (res.next())
            {
                ret = res.getShort(1);
                if (res.wasNull())
                    ret = -1;
            }
            return ret;
        }
        catch (SQLException e)
        {
            throw new THLException("SQL query failed: " + query, e);
        }
        finally
        {
            if (res != null)
                try
                {
                    res.close();
                }
                catch (SQLException ignore)
                {
                }
        }
    }

    /**
     * Return the last applied event as stored in the CommitSeqnoTable.
     * 
     * @return the last applied event, or null if nothing was found
     * @throws THLException
     */
    public ReplDBMSHeader getLastEvent() throws THLException
    {
        if (commitSeqnoTable == null)
            return null;

        try
        {
            return commitSeqnoTable.lastCommitSeqno(taskId);
        }
        catch (SQLException e)
        {
            throw new THLException(e);
        }
    }

}
