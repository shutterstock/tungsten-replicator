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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.Interval;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.enterprise.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.JdbcTHLDatabase;
import com.continuent.tungsten.replicator.thl.THLBinaryEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLEventStatus;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.THLStorage;

/**
 * This class implements an adapter to the disk log. A single instance is shared
 * across all clients.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class DiskTHLStorage implements THLStorage
{
    static Logger           logger               = Logger
                                                         .getLogger(DiskTHLStorage.class);

    // Statistical information.

    // Various operational variables.
    private JdbcTHLDatabase database             = null;
    private DiskLog         diskLog              = null;

    // Settable properties to control this storage implementation.
    protected String        password;
    protected String        url;
    protected String        user;
    protected String        vendor;

    /** Store and compare checksum values on the log. */
    private boolean         doChecksum           = true;

    /** Name of the log directory. */
    protected String        logDirName           = "/opt/tungsten/logs";

    /** Name of the class used to serialize events. */
    protected String        eventSerializerClass = ProtobufSerializer.class
                                                         .getName();

    /** Log file maximum size in bytes. */
    protected int           logFileSize          = 1000000000;

    /** Log file retention in milliseconds. Defaults to 0 (= forever) */
    protected long          logFileRetainMillis  = 0;

    /** Idle log Connection timeout in seconds. */
    protected int           logConnectionTimeout = 28800;

    /** I/O buffer size in bytes. */
    protected int           bufferSize           = 131072;

    /**
     * Flush data after this many milliseconds. 0 flushes after every write.
     */
    private long                flushIntervalMillis        = 0;

    /** If true, fsync when flushing. */
    private boolean             fsyncOnFlush               = false;

    private boolean         readOnly             = true;

    /**
     * Prepare the log for use, which includes ensuring that the log is created
     * automatically on first use and building an index of log file contents.
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Prepare database connection.
        if (url != null && url.length() > 0)
        {
			ReplicatorRuntime runtime = (ReplicatorRuntime) context;
			TungstenProperties conf = context.getReplicatorProperties();
			String metadataSchema = context.getReplicatorSchemaName();
			if (vendor == null && conf != null) // For heterogeneous cases.
				vendor = conf.getString(ReplicatorConf.RESOURCE_VENDOR);
			database = new JdbcTHLDatabase(runtime, null);
			database.connect(url, user, password, metadataSchema, vendor);
			database.prepareSchema();
        }

        // Configure and prepare the log.
        diskLog = new DiskLog();
        diskLog.setDoChecksum(doChecksum);
        diskLog.setEventSerializer(eventSerializerClass);
        diskLog.setLogDir(logDirName);
        diskLog.setLogFileSize(logFileSize);
        diskLog.setLogFileRetainMillis(logFileRetainMillis);
        diskLog.setLogConnectionTimeoutMillis(logConnectionTimeout * 1000);
        diskLog.setBufferSize(bufferSize);
        diskLog.setFlushIntervalMillis(flushIntervalMillis);
        diskLog.setFsyncOnFlush(fsyncOnFlush);
        diskLog.setReadOnly(readOnly);
        diskLog.prepare();

        logger.info("Adapter preparation is complete");
    }

    /**
     * Release database connection and disk log connection(s). {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#release()
     */
    public void release() throws ReplicatorException, InterruptedException
    {
        if (database != null)
        {
            database.close();
            database = null;
        }
        if (diskLog != null)
        {
            diskLog.release();
            diskLog = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#find(long, short)
     */
    public THLEvent find(long seqno, short fragno) throws THLException,
            InterruptedException
    {
        return diskLog.find(seqno, fragno);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxSeqno()
     */
    public long getMaxSeqno() throws THLException
    {
        return diskLog.getMaxSeqno();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setPassword(java.lang.String)
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUrl(java.lang.String)
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUser(java.lang.String)
     */
    public void setUser(String user)
    {
        this.user = user;
    }

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
     * Sets the event serializer name.
     */
    public void setEventSerializer(String eventSerializer)
    {
        this.eventSerializerClass = eventSerializer;
    }

    /**
     * Sets log file retention from an interval string. 
     */
    public void setLogFileRetention(String retention)
    {
        this.logFileRetainMillis = new Interval(retention).longValue();
    }

    /**
     * Sets the interval between flush calls. 
     */
    public void setFlushIntervalMillis(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }
    
    /**
     * If set to true, perform an fsync with every flush. Warning: fsync is very
     * slow, so you want a long flush interval in this case.
     */
    public synchronized void setFsyncOnFlush(boolean fsyncOnFlush)
    {
        this.fsyncOnFlush = fsyncOnFlush;
    }

    /**
     * Set the timeout of idle connections to disk log.
     * 
     * @param logConnectionTimeout Time in milliseconds
     */
    public void setLogConnectionTimeout(int logConnectionTimeout)
    {
        this.logConnectionTimeout = logConnectionTimeout;
    }

    /**
     * Set the read only flag. This indicates whether the log should be opened
     * for read or write access.
     * 
     * @param readOnly
     */
    public void setReadOnly(boolean readOnly)
    {
        this.readOnly = readOnly;
    }

    /**
     * Sets the log buffer size.
     */
    public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#store(com.continuent.tungsten.replicator.thl.THLEvent,
     *      boolean)
     */
    public void store(THLEvent event, boolean doCommit, boolean syncTHL)
            throws THLException, InterruptedException
    {
        diskLog.store(event, doCommit);
        if (doCommit && syncTHL && database != null)
        {
            try
            {
                database.updateCommitSeqnoTable(event);
            }
            catch (SQLException e)
            {
                throw new THLException(
                        "Unable to update commit sequence number: seqno="
                                + event.getSeqno() + " event id="
                                + event.getEventId(), e);
            }
        }
    }

    /**
     * @deprecated {@inheritDoc}
     * @see com.continuent.tungsten.replicator.thl.THLStorage#updateFailedStatus(com.continuent.tungsten.replicator.thl.THLEventStatus,
     *      java.util.ArrayList)
     */
    @Deprecated
    public void updateFailedStatus(THLEventStatus failedEvent,
            ArrayList<THLEventStatus> events) throws THLException
    {
        // TODO: Remove from storage interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * @deprecated {@inheritDoc}
     * @see com.continuent.tungsten.replicator.thl.THLStorage#updateSuccessStatus(java.util.ArrayList,
     *      java.util.ArrayList)
     */
    @Deprecated
    public void updateSuccessStatus(ArrayList<THLEventStatus> succeededEvents,
            ArrayList<THLEventStatus> skippedEvents) throws THLException
    {
        // TODO: Remove from storage interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMinMaxSeqno()
     */
    public long[] getMinMaxSeqno() throws THLException
    {
        // TODO: Remove from storage interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMinSeqno()
     */
    public long getMinSeqno() throws THLException
    {
        return diskLog.getMinSeqno();
    }

    public THLBinaryEvent findBinaryEvent(long seqno, short fragno)
            throws THLException
    {
        // TODO: Decide whether to keep this in the interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getEventId(long)
     */
    public String getEventId(long seqno) throws THLException
    {
        // TODO: Remove from storage interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxCompletedSeqno()
     */
    public long getMaxCompletedSeqno() throws THLException
    {
        // TODO: Remove from storage interface.
        // It is called from only one THL method, which does not seem being
        // called.
        return 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxEventId(java.lang.String)
     */
    public String getMaxEventId(String sourceId) throws THLException
    {
        // TODO: Remove from storage interface.
        // It is called from only one THL method, which does not seem being
        // called.
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxFragno(long)
     */
    public short getMaxFragno(long seqno) throws THLException
    {
        short fragno = 0;
        int timeoutMillis = diskLog.timeoutMillis;

        try
        {
            diskLog.setTimeoutMillis(0);
            while (diskLog.find(seqno, fragno) != null)
            {
                fragno++;
            }
        }
        catch (THLEventNotFoundException expected)
        {
            // No problem here...
        }
        catch (InterruptedException e)
        {
            throw new THLException("Unable to get max fragno for event "
                    + seqno, e);
        }
        finally
        {
            diskLog.setTimeoutMillis(timeoutMillis);

            // Release the connection, otherwise, fetching this event would then
            // fail
            diskLog.connectionManager.releaseConnection();
        }
        return (short) (fragno > 0 ? (fragno - 1) : 0);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setStatus(long,
     *      short, short, java.lang.String)
     */
    public void setStatus(long seqno, short fragno, short status, String msg)
            throws THLException, InterruptedException
    {
        // TODO Never called ?
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#find(long)
     */
    public THLEvent find(long seqno) throws THLException, InterruptedException
    {
        // TODO: Remove from storage interface.
        throw new UnsupportedOperationException("Unsupported method");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#delete(long, long,
     *      String)
     */
    public int delete(Long low, Long high, String before) throws THLException,
            InterruptedException
    {
        diskLog.delete(low, high);
        return -1;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getLastAppliedEvent()
     */
    public ReplDBMSHeader getLastAppliedEvent() throws THLException
    {
        return database.getLastEvent();
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
        return diskLog.setFile(file);
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
    public THLEvent readNextEvent(LogFile file) throws THLException,
            IOException, InterruptedException
    {
        return diskLog.readNextEvent(file);
    }

    /**
     * getIndex returns a String representation of the index, built from the
     * configured log directory.
     * 
     * @return a string representation of the index
     */
    public String getIndex()
    {
        return diskLog.getIndex();
    }
}
