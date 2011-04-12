/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.enterprise.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.enterprise.replicator.thl.serializer.Serializer;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class implements a multi-thread disk log store.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DiskLog
{
    static Logger               logger                     = Logger
                                                                   .getLogger(DiskLog.class);

    // Statistical information.
    private int                 eventCount                 = 0;

    // Various operational variables.
    LogConnectionManager        connectionManager;
    private Serializer          eventSerializer            = null;
    private File                logDir;

    // Variables used to maintain index on log files.
    private LogIndex            index                      = null;
    private long                fileIndex                  = 1;
    private static final int    fileIndexSize              = Integer
                                                                   .toString(
                                                                           Integer.MAX_VALUE)
                                                                   .length();
    private static final String DATA_FILENAME_PREFIX       = "thl.data.";

    /** Store and compare checksum values on the log. */
    private boolean             doChecksum                 = true;

    /** Name of the log directory. */
    protected String            logDirName                 = "/opt/tungsten/logs";

    /** Name of the class used to serialize events. */
    protected String            eventSerializerClass       = ProtobufSerializer.class
                                                                   .getName();

    /** Log file maximum size in bytes. */
    protected int               logFileSize                = 1000000000;

    /** Wait timeout. This is used for testing to prevent infinite timeouts. */
    protected int               timeoutMillis              = Integer.MAX_VALUE;

    /** Number of milliseconds to retain old logs. */
    protected long              logFileRetainMillis        = 0;

    /**
     * Number of milliseconds before timing out idle log connections. Defaults
     * to 8 hours.
     */
    protected int               logConnectionTimeoutMillis = 28800000;

    /** Write lock to prevent log file corruption by concurrent access. */
    protected WriteLock         writeLock;

    /**
     * Creates a new log instance.
     */
    public DiskLog()
    {
    }

    // Log parameters.

    /**
     * Sets the directory that will be used to store the log files
     * 
     * @param path directory to be used. Last / is optional.
     */
    public void setLogDir(String path)
    {
        this.logDirName = path.trim();
        if (this.logDirName.charAt(this.logDirName.length() - 1) != '/')
        {
            this.logDirName = this.logDirName.concat("/");
        }
    }

    /**
     * Sets the log files size. This is approximate as rotation will occur after
     * storing an event that made the file grow above the given limit.
     * 
     * @param size file size
     */
    public void setLogFileSize(int size)
    {
        this.logFileSize = size;
    }

    /**
     * Determines whether to checksum log records.
     * 
     * @param If true use checksums
     */
    public void setDoChecksum(boolean doChecksum)
    {
        this.doChecksum = doChecksum;
    }

    /**
     * Set the number of milliseconds to retain old log files.
     * 
     * @param logFileRetainMillis If other than 0, logs are retained for this
     *            amount of time
     */
    public void setLogFileRetainMillis(long logFileRetainMillis)
    {
        this.logFileRetainMillis = logFileRetainMillis;
    }

    /**
     * Set the number of milliseconds before timing out idle log connections.
     * 
     * @param logConnectionTimeoutMillis Time in milliseconds
     */
    public void setLogConnectionTimeoutMillis(int logConnectionTimeoutMillis)
    {
        this.logConnectionTimeoutMillis = logConnectionTimeoutMillis;
    }

    /**
     * Sets the event serializer name.
     */
    public void setEventSerializer(String eventSerializer)
    {
        this.eventSerializerClass = eventSerializer;
    }

    public void setTimeoutMillis(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
    }

    // Administrative API calls.

    /**
     * Prepare the log for use, which includes ensuring that the log is created
     * automatically on first use and building an index of log file contents.
     */
    public void prepare() throws ReplicatorException, InterruptedException
    {
        logger.info(String.format("Using directory '%s' for replicator logs",
                logDirName));
        logger.info("Checksums enabled for log records: " + doChecksum);

        // Ensure log directory is ready for use, which includes creating
        // a new log directory if desired.
        if (logger.isDebugEnabled())
        {
            logger.debug("logFileSize = " + logFileSize);
        }

        logDir = new File(logDirName);
        if (!logDir.exists())
        {
            logger.info("Log directory does not exist; creating now:"
                    + logDir.getAbsolutePath());
            if (!logDir.mkdirs())
            {
                throw new ReplicatorException(
                        "Unable to create log directory: "
                                + logDir.getAbsolutePath());
            }
        }
        
        if (!logDir.canWrite())
        {
            throw new ReplicatorException("Log directory is not writable: "
                    + logDir.getAbsolutePath());
        }

        // Attempt to acquire write lock.
        File lockFile = new File(logDir, "disklog.lck");
        if (logger.isDebugEnabled())
        {
            logger.debug("Attempting to acquire lock on write lock file: "
                    + lockFile.getAbsolutePath());
        }
        writeLock = new WriteLock(lockFile);
        writeLock.acquire();
        if (writeLock.isLocked())
            logger.info("Acquired write lock; log is writable");
        else
            logger.info("Unable to acquire write lock; log is read-only");

        // Load event serializer.
        try
        {
            eventSerializer = (Serializer) Class.forName(eventSerializerClass)
                    .newInstance();
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to load event serializer class: "
                            + eventSerializerClass, e);
        }
        logger.info("Loaded event serializer class: "
                + eventSerializer.getClass().getName());

        // If the log does not have any files, initialize the first log file
        // now.
        if (LogFile.listLogFiles(logDir, DATA_FILENAME_PREFIX).length == 0)
        {
            String logFileName = getDataFileName(fileIndex);
            LogFile logFile = new LogFile(logDir, logFileName);
            logger.info("Initializing logs: logDir=" + logDir.getAbsolutePath()
                    + " file=" + logFile.getFile().getName());
            logFile.prepareWrite(-1);
        }

        // Create an index on the log.
        if (logger.isDebugEnabled())
            logger.debug("Preparing index");
        index = new LogIndex(logDir, DATA_FILENAME_PREFIX, logFileRetainMillis);

        // Open the last index file and parse the name to get the index of the
        // next file to be created. This ensures new files will be properly
        // created.
        LogFile logFile = openLastFile();
        String logFileName = logFile.getFile().getName();
        int logFileIndexPos = logFile.getFile().getName().lastIndexOf(".");
        fileIndex = Long.valueOf(logFileName.substring(logFileIndexPos + 1));

        // Starting with the sequence number stored in the file header, which
        // is max seqno at time of the log file creation, scan forward through
        // the file and find the last sequence number. At the end of the file,
        // clean up any partially written record(s) to prepare the file for use.
        long maxSeqno = logFile.getBaseSeqno();
        long lastCompleteEventOffset = logFile.getOffset();
        boolean lastFrag = true;

        if (logger.isDebugEnabled())
            logger.debug("Starting max seqno is " + maxSeqno);

        try
        {
            // Read until we find an empty record.
            logger.info("Validating last log file: "
                    + logFile.getFile().getAbsolutePath());
            LogRecord currentRecord = null;

            currentRecord = logFile.readRecord(0);
            byte lastRecordType = -1;
            while (!currentRecord.isEmpty())
            {
                // See what kind of event we have.
                lastRecordType = currentRecord.getData()[0];
                if (lastRecordType == LogRecord.EVENT_REPL)
                {
                    LogEventReplReader eventReader = new LogEventReplReader(
                            currentRecord, eventSerializer, doChecksum);
                    lastFrag = eventReader.isLastFrag();

                    // If we are on a last fragment of an event, update the
                    // last complete transaction offset and store the sequence
                    // number.
                    if (lastFrag)
                    {
                        maxSeqno = eventReader.getSeqno();
                        lastCompleteEventOffset = logFile.getOffset();
                    }
                    eventReader.done();
                }
                else if (lastRecordType == LogRecord.EVENT_ROTATE)
                {
                    // This means the replicator stopped on a rotate log record
                    // with no file beyond it. It is a rare corner case but
                    // we need to handle it or the replicator will not be able
                    // to start writing.
                    logger.info("Last log file ends on rotate log event: "
                            + logFile.getFile().getName());
                    logFile.release();
                    logFile = this.startNewLogFile(maxSeqno + 1);
                    break;
                }

                // Read next record.
                currentRecord = logFile.readRecord(0);
            }

            // Update the index with the max sequence number we found. If the
            // log is empty, this should end up as -1.
            index.setMaxIndexedSeqno(maxSeqno);

            // If the last record is truncated, we have a corrupt file. Fix it
            // now.
            if (currentRecord.isTruncated())
            {
                if (writeLock.isLocked())
                {
                    logger
                            .warn("Log file contains partially written record: offset="
                                    + currentRecord.getOffset()
                                    + " partially written bytes="
                                    + (logFile.getLength() - currentRecord
                                            .getOffset()));
                    logFile.setLength(currentRecord.getOffset());
                    logger
                            .info("Log file truncated to end of last good record: length="
                                    + logFile.getLength());
                }
                else
                {
                    logger.warn("Log ends with a partially written record "
                            + "at end, but this log is read-only.  "
                            + "It is possible that the process that "
                            + "owns the write lock is still writing it.");
                }
            }

            // If the last transaction was not terminated, we need to truncate
            // the log to the end of the last full transaction.
            if (!lastFrag)
            {
                if (writeLock.isLocked())
                {
                    logger
                            .warn("Log file contains partially written transaction; "
                                    + "truncating to last full transaction: seqno="
                                    + maxSeqno
                                    + " length="
                                    + lastCompleteEventOffset);
                    logFile.setLength(lastCompleteEventOffset);
                }
                else
                {
                    logger.warn("Log ends with a partially written "
                            + "transaction, but this log is read-only.  "
                            + "It is possible that the process that "
                            + "owns the write lock is still writing it.");
                }
            }
        }
        catch (IOException e)
        {
            throw new ReplicatorException(
                    "I/O error while scanning log file: name="
                            + logFile.getFile().getAbsolutePath() + " offset="
                            + logFile.getOffset(), e);
        }
        finally
        {
            if (logFile != null)
                logFile.release();
        }

        // Open up the connection manager for business.
        this.connectionManager = new LogConnectionManager();
        connectionManager.setTimeoutMillis(logConnectionTimeoutMillis);
        logger.info(String.format("Idle log connection timeout: %dms",
                logConnectionTimeoutMillis));

        logger.info("Log preparation is complete");
    }

    /**
     * Releases the log resources. This should be called after use.
     */
    public void release() throws ReplicatorException, InterruptedException
    {
        writeLock.release();
        connectionManager.release();
    }

    /**
     * Return the maximum sequence number stored in the log.
     */
    public long getMaxSeqno() throws THLException
    {
        if (logger.isDebugEnabled())
            logger.debug("Getting max seqno for thread "
                    + Thread.currentThread().getName() + "("
                    + Thread.currentThread().getId() + ") using " + this);
        return index.getMaxIndexedSeqno();
    }

    /**
     * Return the minimum sequence number stored in the log.
     */
    public long getMinSeqno()
    {
        return index.getMinIndexedSeqno();
    }

    /**
     * Returns the count of files in the log.
     * 
     * @return
     */
    public int fileCount()
    {
        return index.size();
    }

    // API to read and write the log.

    /**
     * Finds a specific THLEvent.
     * 
     * @param seqno Desired sequence number
     * @param fragno Desired fragment
     */
    public THLEvent find(long seqno, short fragno) throws THLException,
            InterruptedException
    {
        // Fetch matching log record for this seqno/fragno.
        LogRecord logRecord = findEventLogRecord(seqno, fragno);
        if (logRecord == null)
            return null;

        // Fetch the stored event out and return same.
        LogEventReplReader eventReader = new LogEventReplReader(logRecord,
                eventSerializer, doChecksum);
        THLEvent event = eventReader.deserializeEvent();
        eventReader.done();
        return event;
    }

    /**
     * Store a THL event at the end of the log.
     */
    public void store(THLEvent event, boolean syncCommitSeqno)
            throws THLException, InterruptedException
    {
        // Ensure that the log is writable.
        if (!writeLock.isLocked())
        {
            throw new THLException("Attempt to write to read-only log: seqno="
                    + event.getSeqno() + " fragno=" + event.getFragno());
        }

        // If the log does not have any files, initialize the first log file
        // now.
        if (index.size() == 0)
        {
            try
            {
                startNewLogFile(event.getSeqno());
            }
            catch (IOException e)
            {
                throw new THLException("Unable to initialize log", e);
            }
        }

        LogConnection logConnection = connectionManager.getLogConnection(event
                .getSeqno());
        try
        {
            if (logConnection == null)
            {
                try
                {
                    LogFile lastFile = openLastFile();
                    logConnection = connectionManager
                            .createAndGetLogConnection(lastFile, event
                                    .getSeqno());
                }
                catch (ReplicatorException e)
                {
                    throw new THLException("Failed to open log last log file",
                            e);
                }
            }

            // Retrieve the log file and optionally note the name.
            LogFile dataFile = logConnection.getLogFile();
            if (logger.isDebugEnabled())
            {
                logger.debug("Using log file for writing: "
                        + dataFile.getFile().getName());
            }

            eventCount++;
            try
            {
                // Write the new record into the log.
                // See if we need to rotate the file. This should only happen
                // on a full transaction boundary, not in the middle of a
                // fragmented transactions.
                if (dataFile.getLength() > logFileSize
                        && event.getFragno() == 0)
                {
                    dataFile = rotate(dataFile, event.getSeqno());
                    logConnection = connectionManager
                            .createAndGetLogConnection(dataFile, event
                                    .getSeqno());
                }

                // Write the event to byte stream.
                LogEventReplWriter eventWriter = new LogEventReplWriter(event,
                        eventSerializer, doChecksum);
                LogRecord logRecord = eventWriter.write();

                // Write to the file.
                dataFile.writeRecord(logRecord, logFileSize);
                index.setMaxIndexedSeqno(event.getSeqno());

                // If it is time to commit, make it happen!
                if (syncCommitSeqno)
                {
                    // TODO: Ensure that DBMS pointer is updated.
                    // dataFile.fsync();
                }
            }
            catch (IOException e)
            {
                throw new THLException("Error while writing to log file: name="
                        + dataFile.getFile().getName(), e);

            }

        }
        finally
        {
            // Ensure our connection is properly returned.
            if (logConnection != null)
            {
                connectionManager.returnLogConnection(logConnection);
            }
        }
    }

    /**
     * Find the event log record corresponding to a particular sequence number
     * and fragno. This method automatically handles scanning to the next file.
     * This method has an indefinite timeout, hence will wait forever until the
     * event appears.
     * 
     * @param seqno Log sequence number
     * @param fragno Fragment number
     * @return The corresponding record or null if not found.
     */
    private LogRecord findEventLogRecord(long seqno, short fragno)
            throws InterruptedException, THLException
    {
        // Find open file for this thread and ensure if it is the right file
        // for this sequence number. If not, switch files.
        LogConnection logConnection = connectionManager.getLogConnection(seqno);

        try
        {
            // We didn't get a connection, so we need to generate a new file.
            if (logConnection == null)
            {
                String newFileName = index.getFile(seqno);
                if (newFileName == null)
                {
                    // If we cannot get a file name, that means the log does
                    // not have this sequence number.
                    if (logger.isDebugEnabled())
                    {
                        logger
                                .debug("Requested seqno does not exist in log: seqno="
                                        + seqno);
                    }
                    return null;
                }
                logger
                        .debug("No read connection for thread; allocating new one: thread="
                                + Thread.currentThread().getId()
                                + " seqno="
                                + seqno + " logFile=" + newFileName);
                LogFile data1 = new LogFile(logDir, newFileName);
                data1.prepareRead();
                logConnection = connectionManager.createAndGetLogConnection(
                        data1, seqno);
            }

            // Retrieve the log file and optionally note the name.
            LogFile data = logConnection.getLogFile();
            if (logger.isDebugEnabled())
            {
                logger.debug("Using log file for read: "
                        + data.getFile().getName());
            }

            // Scan for the sequence number.
            boolean found = false;

            while (!found)
            {
                try
                {
                    LogRecord logRecord = data.readRecord(timeoutMillis);

                    // Timeouts return an empty record. In that case we return
                    // null, because the record was not found. Otherwise, we
                    // are highly surprised.
                    if (logRecord.isEmpty())
                    {
                        throw new THLEventNotFoundException(
                                "Unexpected empty record received from log while searching for seqno: name="
                                        + data.getFile().getName() + " offset="
                                        + logRecord.getOffset() + " seqno="
                                        + seqno + " fragno=" + fragno);
                    }

                    byte[] bytes = logRecord.getData();
                    byte recordType = bytes[0];
                    if (recordType == LogRecord.EVENT_REPL)
                    {
                        LogEventReplReader eventReader = new LogEventReplReader(
                                logRecord, eventSerializer, doChecksum);

                        if (eventReader.getSeqno() == seqno
                                && eventReader.getFragno() == fragno)
                        {
                            // Found the event.
                            return logRecord;
                        }
                        else if (eventReader.getSeqno() > seqno
                                || (eventReader.getSeqno() == seqno && eventReader
                                        .getFragno() > fragno))
                        {
                            // This event does not exist in the log.
                            return null;
                        }
                    }
                    else if (recordType == LogRecord.EVENT_ROTATE)
                    {
                        // We are at the end of the current file and need to
                        // move to the next file.
                        if (logger.isDebugEnabled())
                            logger.debug("Found a rotate event: file="
                                    + data.getFile().getName() + " offset="
                                    + logRecord.getOffset());

                        // Read the header and get the next file name.
                        LogEventRotateReader rotateReader = new LogEventRotateReader(
                                logRecord, doChecksum);
                        String newFileName = getDataFileName(rotateReader
                                .getIndex());

                        // Use this to allocate the next LogFile and create a
                        // connection for it so we can continue reading.
                        data = new LogFile(logDir, newFileName);
                        data.prepareRead();
                        logConnection = connectionManager
                                .createAndGetLogConnection(data, seqno);
                    }
                    else
                    {
                        throw new THLException(
                                "Unable to extract a valid record type; log appears to be corrupted: file="
                                        + data.getFile().getName() + " offset="
                                        + logRecord.getOffset()
                                        + " record type=" + recordType);
                    }

                }
                catch (IOException e)
                {
                    throw new THLException("Failed to extract event from log",
                            e);
                }
            }
            return null;
        }
        finally
        {
            if (logConnection != null)
                connectionManager.returnLogConnection(logConnection);
        }
    }

    /**
     * Validates the log to ensure there are no inconsistencies.
     * 
     * @throws LogConsistencyException Thrown if log is not consistent
     */
    public void validate() throws LogConsistencyException
    {
        index.validate(logDir);
    }

    /**
     * Deletes a portion of the log. This operation requires a file lock to
     * accomplish.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the current beginning of the log.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to delete to the end of the log.
     * @throws THLException
     * @param low
     * @param high
     */
    public void delete(Long low, Long high) throws THLException,
            InterruptedException
    {
        // Ensure the log is writable.
        if (!writeLock.isLocked())
        {
            throw new THLException("Attempt to delete from read-only log");
        }

        // Determine the range of sequence numbers to delete.
        long lowSeqno;
        long highSeqno;
        if (low == null)
            lowSeqno = index.getMinIndexedSeqno();
        else
            lowSeqno = low;
        if (high == null)
            highSeqno = index.getMaxIndexedSeqno();
        else
            highSeqno = high;

        // For now we don't permit logs to be deleted from the middle as this
        // would result in corruption.
        if (highSeqno != index.getMaxIndexedSeqno()
                && lowSeqno != index.getMinIndexedSeqno())
        {
            throw new THLException("Deletion range invalid; "
                    + "must include one or both log end points: low seqno="
                    + lowSeqno + " high seqno=" + highSeqno);
        }

        // Start reading through the available log files one index at a time.
        for (LogIndexEntry lie : index.getIndexCopy())
        {
            if (lie.startSeqno >= lowSeqno && lie.endSeqno <= highSeqno)
            {
                logger.info("Deleting log file: " + lie.toString());
                purgeFile(lie);
            }
            else if (lie.startSeqno < lowSeqno && lie.endSeqno >= lowSeqno)
            {
                // Upper end of file is in delete range, so we truncate.
                logger.info("Truncating log file at seqno " + lowSeqno + ": "
                        + lie.toString());
                truncateFile(lie, lowSeqno);
            }
        }
    }

    // Drops a file completely.
    private void purgeFile(LogIndexEntry entry)
    {
        index.removeFile(entry.fileName);
        File f = new File(logDir, entry.fileName);
        if (!f.delete())
        {
            logger.warn("Unable to delete log file: " + f.getAbsolutePath());
        }
    }

    // Truncates the file at a particular sequence number.
    private void truncateFile(LogIndexEntry entry, long seqno)
            throws THLException, InterruptedException
    {
        LogFile logFile = null;
        try
        {
            logFile = openFile(entry.fileName);
            long offset = logFile.getOffset();
            LogRecord currentRecord = logFile.readRecord(0);
            while (!currentRecord.isEmpty())
            {

                // See what kind of event we have.
                byte recordType = currentRecord.getData()[0];
                if (recordType == LogRecord.EVENT_REPL)
                {
                    LogEventReplReader eventReader = new LogEventReplReader(
                            currentRecord, eventSerializer, doChecksum);
                    long currentSeqno = eventReader.getSeqno();
                    eventReader.done();

                    if (currentSeqno >= seqno)
                    {
                        // This means we found the truncation point.
                        logger
                                .info("Truncating log file after sequence number: file="
                                        + entry.fileName + " seqno=" + seqno);
                        logFile.setLength(offset);
                        index.setMaxIndexedSeqno(seqno - 1);
                        break;
                    }
                }
                else if (recordType == LogRecord.EVENT_ROTATE)
                {
                    // This means we hit the end of the file without truncating.
                    logger
                            .warn("Unable to truncate log file at intended sequence number: file="
                                    + entry.fileName + " seqno=" + seqno);
                    break;
                }

                // Remember current offset and read the next record.
                offset = logFile.getOffset();
                currentRecord = logFile.readRecord(0);
            }
        }
        catch (IOException e)
        {
            throw new THLException(
                    "Unable to read log file: " + entry.fileName, e);
        }
        catch (ReplicatorException e)
        {
            throw new THLException("Unable to process log file: "
                    + entry.fileName, e);
        }
        finally
        {
            if (logFile != null)
                logFile.release();
        }
    }

    /**
     * Open the last log file for writing. The file is assumed to exist as the
     * log must be initialized at this point.
     * 
     * @return a {@link LogFile} object referencing the last indexed file
     * @throws ReplicatorException if an error occurs
     */
    private LogFile openLastFile() throws ReplicatorException
    {
        String logFileName = index.getLastFile();
        return openFile(logFileName);
    }

    /**
     * Open a specific log file for writing.
     * 
     * @param logFileName Log file name
     * @return a {@link LogFile} object referencing the last indexed file
     * @throws ReplicatorException if an error occurs
     */
    private LogFile openFile(String logFileName) throws ReplicatorException
    {
        // Get the file reference.
        LogFile data = new LogFile(logDir, logFileName);

        // Ensure the file exists.
        if (!data.getFile().exists())
        {
            throw new ReplicatorException(
                    "Last log file does not exist; index may be corrupt: "
                            + data.getFile().getName());
        }

        // Open for writing. The file exists so we pass in -1 for the
        // sequence number because we won't write it in the header. This
        // is hacky but should work.
        if (logger.isDebugEnabled())
            logger.debug("Opening log file: "
                    + data.getFile().getAbsolutePath());
        data.prepareWrite(-1);

        return data;
    }

    /**
     * Rotate to the next file to store data : write the rotate event, close the
     * file and prepare the new one, if it does not exists
     * 
     * @dataFile Data file to be rotated
     * @seqno Sequence number of first event in new file
     */
    private LogFile rotate(LogFile dataFile, long seqno) throws IOException,
            THLException, InterruptedException
    {
        // Increment the log index here.
        fileIndex = (fileIndex + 1) % Integer.MAX_VALUE;
        writeRotateEvent(dataFile);
        return startNewLogFile(seqno);
    }

    /**
     * Write a rotate event at the end of the given file
     * 
     * @param dataFile the file where the rotate event is going to be written
     * @throws THLException
     */
    private void writeRotateEvent(LogFile dataFile) throws THLException,
            InterruptedException
    {
        // Write the new record into the log.
        try
        {
            LogEventRotateWriter writer = new LogEventRotateWriter(fileIndex,
                    doChecksum);
            LogRecord logRec = writer.write();
            dataFile.writeRecord(logRec, 0);
        }
        catch (IOException e)
        {
            throw new THLException(
                    "Error writing rotate log event to log file: name="
                            + dataFile.getFile().getName(), e);
        }
    }

    /**
     * Start a new log file.
     * 
     * @seqno Sequence number of first event in the file
     */
    private LogFile startNewLogFile(long seqno) throws THLException,
            IOException
    {
        // Open new log file and update index. TODO: did this get updated?
        String logFileName = getDataFileName(fileIndex);
        LogFile dataFile = new LogFile(logDir, logFileName);
        if (dataFile.getFile().exists())
        {
            throw new THLException("New log file exists already: "
                    + dataFile.getFile().getName());
        }
        dataFile.prepareWrite(seqno);

        // Add the file to the volatile index.
        index.addNewFile(seqno, logFileName);

        return dataFile;
    }

    /**
     * Returns the name of a log file based on an index
     * 
     * @return a file name corresponding to the given index
     */
    private String getDataFileName(long index)
    {
        return DATA_FILENAME_PREFIX
                + String.format("%0" + fileIndexSize + "d", index);
    }

    /**
     * setFile prepares a log file to be read.
     * 
     * @param file Name of the file to be prepared. This file must be found in
     *            the configured logs directory.
     * @return the log file descriptor if found
     * @throws ReplicatorException in case of error
     */
    public LogFile setFile(String file) throws ReplicatorException
    {
        LogFile data = new LogFile(logDir, file);
        data.prepareRead();
        return data;
    }

    /**
     * Read the next event from the provided file.
     * 
     * @param data a log file descriptor
     * @return a THLEvent extracted from the file, or null if no more THLEvent
     *         could be decoded from the file.
     * @throws THLException
     * @throws IOException
     * @throws InterruptedException
     */
    public THLEvent readNextEvent(LogFile data) throws THLException,
            IOException, InterruptedException
    {
        LogRecord logRecord = data.readRecord(LogFile.NO_WAIT);

        // Timeouts return an empty record. In that case we return
        // null, because the record was not found. Otherwise, we
        // are highly surprised.
        if (logRecord.isEmpty())
        {
            return null;
        }

        byte[] bytes = logRecord.getData();
        byte recordType = bytes[0];
        if (recordType == LogRecord.EVENT_REPL)
        {
            LogEventReplReader eventReader = new LogEventReplReader(logRecord,
                    eventSerializer, doChecksum);

            THLEvent event = eventReader.deserializeEvent();
            eventReader.done();
            return event;
        }
        else
        {
            return null;
        }
    }

    /**
     * getIndex returns a String representation of the index, built from the
     * configured log directory.
     * 
     * @return a string representation of the index
     */
    public String getIndex()
    {
        return index.toString();
    }
}