/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.thl.log;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * Implements client operations on the log. Each individual client of the log
 * must instantiate a separate connection. The client must be released after use
 * to avoid resource leaks.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LogConnection
{
    private static Logger      logger        = Logger.getLogger(LogConnection.class);

    /**
     * Simple representing base seqno of uninitialized log.
     */
    public static long         UNINITIALIZED = -1;

    /**
     * Symbol representing the first seqno in a new log.
     */
    public static long         FIRST         = 0;

    // Client connection parameters.
    private final boolean      readonly;

    // Control parameters.
    private volatile boolean   done          = false;

    // Disk log parameters.
    private DiskLog            diskLog;
    private LogCursor          cursor;
    private THLEvent           pendingEvent;
    private long               pendingSeqno;
    private long               writeCount    = 0;
    private long               readCount     = 0;

    // Information required for successful output.
    private boolean            doChecksum;
    private Serializer         eventSerializer;
    private int                logFileSize;
    private int                timeoutMillis;
    private int                logRotateMillis;

    // Filter used to decide whether to deserialize events on input.
    private LogEventReadFilter readFilter;

    /**
     * Instantiates a client on a disk log.
     * 
     * @param disklog Disk log we are accessing
     * @param readonly If true, this client may not write
     */
    LogConnection(DiskLog diskLog, boolean readonly)
    {
        this.diskLog = diskLog;
        this.readonly = readonly;

        // Fetch log information for reads.
        this.eventSerializer = diskLog.getEventSerializer();
        this.doChecksum = diskLog.isDoChecksum();
        this.timeoutMillis = diskLog.getTimeoutMillis();
        this.logRotateMillis = diskLog.getLogRotateMillis();

        // Fetch log information required to handle writes if needed.
        if (!readonly)
        {
            this.logFileSize = diskLog.getLogFileSize();
        }
    }

    /**
     * Returns true if this is a read-only client.
     */
    public boolean isReadonly()
    {
        return readonly;
    }

    /**
     * Sets the read filter, which determines whether events are fully
     * deserialized on read. This implements query logic on scanned events.
     */
    public void setReadFilter(LogEventReadFilter readFilter)
    {
        this.readFilter = readFilter;
    }

    /**
     * Sets the timeout in milliseconds for blocking reads on this connection.
     * The value overrides the read timeout for the log as a whole.
     */
    public void setTimeoutMillis(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Sets the local connection value for reading a new file after a log
     * rotation.
     */
    public void setLogRotateMillis(int logRotateMillis)
    {
        this.logRotateMillis = logRotateMillis;
    }

    /**
     * Releases the client connection. This must be called to avoid resource
     * leaks.
     */
    public void release()
    {
        if (diskLog != null)
            diskLog.release(this);
    }

    /**
     * Releases the client connection. This must be called to avoid resource
     * leaks.
     */
    public synchronized void releaseInternal()
    {
        if (!done)
        {
            if (cursor != null)
            {
                cursor.release();
                cursor = null;
            }
            diskLog = null;
            done = true;
        }
    }

    /**
     * Returns true if connection is no longer in use.
     */
    public boolean isDone()
    {
        return done;
    }

    /**
     * Finds a specific THLEvent and position client cursor on the event.
     * 
     * @param seqno Desired sequence number
     * @param fragno Desired fragment
     * @return True if seek is successful and next() may be called; false if
     *         event does not exist
     * @throws ReplicatorException thrown if log cannot be read
     */
    public synchronized boolean seek(long seqno, short fragno)
            throws ReplicatorException, InterruptedException
    {
        assertNotDone();

        // If we have a previous read state, clear it now.
        if (cursor != null)
        {
            cursor.release();
            cursor = null;
        }
        pendingEvent = null;
        pendingSeqno = UNINITIALIZED;

        // Find the log file that contains our sequence number.
        LogFile logFile = diskLog.getLogFile(seqno);
        if (logFile == null)
        {
            // If we cannot get the log file, that means the log does
            // not exist.
            if (logger.isDebugEnabled())
            {
                logger.debug("Log is uninitialized and does not contain seqno: seqno="
                        + seqno);
            }
            return false;
        }

        // Open the file for reading and allocate a cursor.
        logFile.openRead();
        cursor = new LogCursor(logFile, seqno);
        cursor.setRotateNext(true);
        if (logger.isDebugEnabled())
        {
            logger.debug("Using log file " + logFile.getFile().getName()
                    + " - seeking event " + seqno + "/" + fragno);
        }

        // If we are looking for the first sequence number, we can stop now that
        // the first log file is open.
        if (seqno == FIRST)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Seeking seqno in newly initialized log: seqno="
                        + seqno);
            }
            pendingSeqno = seqno;
            return true;
        }

        // Look for the sequence number we are trying to find.
        long lastSeqno = logFile.getBaseSeqno();
        while (true)
        {
            try
            {
                // Look for the record. If it is empty, we did not find the
                // sequence number.
                LogRecord logRecord = logFile.readRecord(0);
                if (logRecord.isEmpty())
                {
                    // If we are positioned on the end of the log, this means we
                    // must be waiting for the record to arrive. We are
                    // correctly positioned.
                    if (lastSeqno == UNINITIALIZED && lastSeqno < 0)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Seeking seqno in newly initialized log: seqno="
                                    + seqno);
                        }
                        pendingSeqno = seqno;
                        return true;
                    }
                    else if (seqno == (lastSeqno + 1))
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Seeking seqno past end of log: seqno="
                                    + seqno + " end seqno=" + lastSeqno);
                        }
                        pendingSeqno = seqno;
                        return true;
                    }
                    else
                        break;
                }

                byte[] bytes = logRecord.getData();
                byte recordType = bytes[0];
                if (recordType == LogRecord.EVENT_REPL)
                {
                    // We have an event. Check the header.
                    LogEventReplReader eventReader = new LogEventReplReader(
                            logRecord, eventSerializer, doChecksum);

                    if (eventReader.getSeqno() == seqno
                            && eventReader.getFragno() == fragno)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Found requested event (" + seqno
                                    + "/" + fragno + ")");
                        // We found the event we are looking for.
                        pendingEvent = deserialize(logRecord);
                        break;
                    }
                    else if (eventReader.getSeqno() > seqno
                            || (eventReader.getSeqno() == seqno && eventReader
                                    .getFragno() > fragno))
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Requested event (" + seqno + "/"
                                    + fragno + ") not found. Found event "
                                    + eventReader.getSeqno() + "/"
                                    + eventReader.getFragno() + " instead");

                        // Our event is simply not in the log.
                        break;
                    }
                    else
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Requested event (" + seqno + "/"
                                    + fragno
                                    + ") not reached. Current position "
                                    + eventReader.getSeqno() + "/"
                                    + eventReader.getFragno());

                        // Remember which seqno we saw and keep going.
                        lastSeqno = eventReader.getSeqno();
                    }
                }
                else if (recordType == LogRecord.EVENT_ROTATE)
                {
                    // We are on a rotate log event. This means the event is not
                    // there.
                    break;
                }
                else
                {
                    // We land here if the file is bad.
                    throw new THLException(
                            "Unable to extract a valid record type; log appears to be corrupted: file="
                                    + logFile.getFile().getName() + " offset="
                                    + logRecord.getOffset() + " record type="
                                    + recordType);
                }

            }
            catch (IOException e)
            {
                throw new THLException("Failed to extract event from log", e);
            }
        }

        // If we have a pending event, the seek was successful.
        return (pendingEvent != null);
    }

    // Deserialize the event we just found. This takes into consideration
    // the read filter, if present.
    private THLEvent deserialize(LogRecord logRecord)
            throws ReplicatorException
    {
        LogEventReplReader eventReader = new LogEventReplReader(logRecord,
                eventSerializer, doChecksum);
        THLEvent event;

        // If there is no read filter or if the filter asks us to accept, then
        // deserialize fully. Otherwise generate a THLEvent from the header
        // information only.
        if (readFilter == null || readFilter.accept(eventReader))
        {
            event = eventReader.deserializeEvent();
        }
        else
        {
            event = new THLEvent(eventReader.getSeqno(),
                    eventReader.getFragno(), eventReader.isLastFrag(),
                    eventReader.getSourceId(), THLEvent.REPL_DBMS_EVENT,
                    eventReader.getEpochNumber(), new Timestamp(
                            System.currentTimeMillis()), new Timestamp(
                            eventReader.getSourceTStamp()),
                    eventReader.getEventId(), eventReader.getShardId(), null);
        }

        eventReader.done();
        return event;
    }

    /**
     * Positions cursor on first fragment of a specific event.
     * 
     * @param seqno Desired sequence number
     * @return True if seek is successful and next() may be called; false if
     *         event does not exist
     * @throws ReplicatorException thrown if log cannot be read
     */
    public synchronized boolean seek(long seqno) throws ReplicatorException,
            InterruptedException
    {
        return seek(seqno, (short) 0);
    }

    /**
     * Opens a log file and positions client cursor on the event. Clients may
     * call next to read events.
     * 
     * @param name The short name of a current log file
     * @return True if seek is successful and next() may be called
     * @throws ReplicatorException Thrown if the log cannot be read
     * @throws IOException Thrown if file cannot be found
     * @throws InterruptedException
     */
    public synchronized boolean seek(String name) throws ReplicatorException,
            IOException, InterruptedException
    {
        assertNotDone();

        // Clear any pending state.
        clearReadState();

        // Try to seek on the file.
        LogFile logFile = diskLog.getLogFile(name);
        if (logFile == null)
            return false;
        else
        {
            logFile.openRead();
            cursor = new LogCursor(logFile, logFile.getBaseSeqno());
            cursor.setRotateNext(false);
            if (logger.isDebugEnabled())
            {
                logger.debug("Using log file for read: "
                        + logFile.getFile().getName());
            }
            pendingSeqno = logFile.getBaseSeqno();
            return true;
        }
    }

    // Clear read state prior to seek.
    private void clearReadState()
    {
        if (cursor != null)
        {
            cursor.release();
            cursor = null;
        }
        if (pendingEvent != null)
            pendingEvent = null;

    }

    /**
     * Returns the next event in the log. If blocking is enabled, this will wait
     * for a new event to arrive. If disabled, this call returns immediately if
     * there is no next event.
     * 
     * @param block If true, read blocks until next event is available
     * @return A THLEvent or null if we are non-blocking
     */
    public synchronized THLEvent next(boolean block)
            throws ReplicatorException, InterruptedException
    {
        assertNotDone();

        // Ensure we have a cursor from a previous seek.
        if (cursor == null)
        {
            throw new THLException(
                    "Must seek before attempting to read next event");
        }

        // If we have a pending event, just hand that back.
        if (pendingEvent != null)
        {
            THLEvent event = pendingEvent;
            pendingEvent = null;
            readCount++;
            return event;
        }

        // Retrieve the log file and optionally note the name.
        LogFile data = cursor.getLogFile();
        if (logger.isDebugEnabled())
        {
            logger.debug("Using log file " + data.getFile().getName()
                    + " - reading event");
        }

        // Set the timeout value.
        int readTimeoutMillis = 0;
        if (block)
            readTimeoutMillis = timeoutMillis;

        // Scan for the record.
        THLEvent event = null;
        while (event == null)
        {
            try
            {
                LogRecord logRecord = data.readRecord(readTimeoutMillis);

                // Timeouts return an empty record. In that case we return
                // null, because the record was not found.
                if (logRecord.isEmpty())
                {
                    return null;
                }

                byte[] bytes = logRecord.getData();
                byte recordType = bytes[0];
                if (recordType == LogRecord.EVENT_REPL)
                {
                    event = deserialize(logRecord);
                    break;
                }
                else if (recordType == LogRecord.EVENT_ROTATE)
                {
                    // We are at the end of the current file and need to
                    // move to the next file.
                    if (logger.isDebugEnabled())
                        logger.debug("Found a rotate event: file="
                                + data.getFile().getName() + " offset="
                                + logRecord.getOffset());

                    // If we are reading just one log with no rotations, this is
                    // the end of the road.
                    if (!cursor.isRotateNext())
                        return null;

                    // Otherwise read the rotate event and get the next file
                    // name.
                    LogEventRotateReader rotateReader = new LogEventRotateReader(
                            logRecord, doChecksum);
                    String newFileName = diskLog.getDataFileName(rotateReader
                            .getIndex());

                    // Release current cursor to free OS file descriptor.
                    cursor.release();

                    // Attempt to open the next log file. This is subject to a
                    // timeout as the log may be truncated after the current log
                    // file or we may be reading an active log and just happen
                    // to look for the next file before the writer can finish
                    // flushing the first write to disk.
                    int rotationTimeout = logRotateMillis;
                    while (rotationTimeout > 0)
                    {
                        // Try to open file, exiting loop if successful.
                        data = diskLog.getLogFileForReading(newFileName);
                        if (data != null)
                            break;

                        // Non-blocking reads just return a null.
                        if (data == null && !block)
                        {
                            // TODO: Reconnect to the log file, as we have just
                            // messed up our position so that this call is not
                            // idempotent.
                            return null;
                        }

                        // Blocking reads sleep for 50ms.
                        long startSleepMillis = System.currentTimeMillis();
                        Thread.sleep(50);
                        long sleepMillis = System.currentTimeMillis()
                                - startSleepMillis;
                        rotationTimeout -= sleepMillis;
                        if (rotationTimeout <= 0)
                            throw new LogTimeoutException(
                                    "Read timed out while waiting for rotated log file; "
                                            + "this may indicate log corruption: missing file="
                                            + newFileName);
                    }

                    // Open cursor on next file.
                    cursor = new LogCursor(data, -1);
                    cursor.setRotateNext(true);
                }
                else
                {
                    throw new THLException(
                            "Unable to extract a valid record type; log appears to be corrupted: file="
                                    + data.getFile().getName() + " offset="
                                    + logRecord.getOffset() + " record type="
                                    + recordType);
                }

            }
            catch (IOException e)
            {
                throw new THLException("Failed to extract event from log", e);
            }
        }

        // We now have an event. If this is the first read after a seek, make
        // sure we found what we expected.
        if (pendingSeqno != UNINITIALIZED)
        {
            if (event.getSeqno() != pendingSeqno)
                throw new LogPositionException(
                        "Log seek failure: expected seqno=" + pendingSeqno
                                + " found seqno=" + event.getSeqno());
            pendingSeqno = UNINITIALIZED;
        }

        // Increment read count and return.
        readCount++;
        return event;
    }

    /**
     * Convenience method to return the next event with blocking enabled.
     * 
     * @return A THLEvent or null if we are non-blocking
     */
    public synchronized THLEvent next() throws ReplicatorException,
            InterruptedException
    {
        return next(true);
    }

    /**
     * Store a THL event at the end of the log.
     * 
     * @param event THLEvent to store
     * @param commit If true, flush to storage
     */
    public synchronized void store(THLEvent event, boolean commit)
            throws ReplicatorException, InterruptedException
    {
        assertWritable();

        // TODO: Ensure that a log file is open. We should not grant connections
        // if log is uninitialized.

        // If we do not have a cursor, create one now.
        if (this.cursor == null)
        {
            try
            {
                LogFile lastFile = diskLog.openLastFile(false);
                cursor = new LogCursor(lastFile, event.getSeqno());
                if (logger.isDebugEnabled())
                {
                    logger.debug("Creating new log cursor: thread="
                            + Thread.currentThread() + " file="
                            + lastFile.getFile().getName() + " seqno="
                            + event.getSeqno());
                }
            }
            catch (ReplicatorException e)
            {
                throw new THLException("Failed to open log last log file", e);
            }
        }

        // Retrieve the log file and optionally note the name.
        LogFile dataFile = cursor.getLogFile();
        if (logger.isDebugEnabled())
        {
            logger.debug("Using log file for writing: "
                    + dataFile.getFile().getName());
        }

        try
        {
            // See if we need to rotate the file. This should only happen
            // on a full transaction boundary, not in the middle of a
            // fragmented transaction.
            if (dataFile.getLength() > logFileSize && event.getFragno() == 0)
            {
                dataFile = diskLog.rotate(dataFile, event.getSeqno());
                cursor.release();
                cursor = new LogCursor(dataFile, event.getSeqno());
            }

            // Write the event to byte stream.
            LogEventReplWriter eventWriter = new LogEventReplWriter(event,
                    eventSerializer, doChecksum);
            LogRecord logRecord = eventWriter.write();

            // Write to the file.
            dataFile.writeRecord(logRecord, logFileSize);
            diskLog.setMaxSeqno(event.getSeqno());
            writeCount++;

            // If it is time to commit, make it happen!
            if (commit)
            {
                dataFile.flush();
            }
        }
        catch (IOException e)
        {
            throw new THLException("Error while writing to log file: name="
                    + dataFile.getFile().getName(), e);

        }

    }

    /**
     * Commit transactions stored in the log.
     */
    public synchronized void commit() throws ReplicatorException,
            InterruptedException
    {
        assertWritable();

        // If we have an active cursor, issue a commit now.
        if (cursor != null)
        {
            // Issue a flush call.
            LogFile dataFile = cursor.getLogFile();
            try
            {
                dataFile.flush();
            }
            catch (IOException e)
            {
                throw new THLException("Commit failed on log: seqno="
                        + cursor.getLastSeqno() + " log file="
                        + dataFile.getFile().getName());
            }

            // This is a good time to make sure the sync thread is running.
            diskLog.checkLogSyncTask();
        }
    }

    /**
     * Rollback transactions stored in the log.
     */
    public synchronized void rollback() throws ReplicatorException
    {
        // TODO: Implement rollback.
        assertWritable();
    }

    /**
     * Delete a range of events from the log.
     */
    public synchronized void delete(Long low, Long high)
            throws ReplicatorException, InterruptedException
    {
        assertWritable();
        diskLog.delete(this, low, high);
    }

    // Ensure this is a writable connection.
    private void assertWritable() throws ReplicatorException
    {
        assertNotDone();
        if (readonly)
        {
            throw new THLException(
                    "Attempt to write using read-only log connection");
        }
        if (!diskLog.isWritable())
        {
            throw new THLException(
                    "Attempt to write using read-only log connection");
        }
    }

    // Ensure we are not released.
    private void assertNotDone() throws ReplicatorException
    {
        if (done)
            throw new THLException("Attempt to use released connection");
    }
}