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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Manages log connections behalf of threads. Threads either fetch existing
 * connections or allocate new ones.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogConnectionManager
{
    private static Logger            logger           = Logger
                                                              .getLogger(LogConnectionManager.class);

    // Map of active connections and flag to indicate we are closed for
    // business.
    private Map<Long, LogConnection> connections      = new Hashtable<Long, LogConnection>();
    private boolean                  done;

    // Connection timeout variables.
    private long                     lastTimeoutCheck = System
                                                              .currentTimeMillis();
    private int                      timeoutMillis    = 5000;
    private long                     nextTimeoutCheck;

    /**
     * Create a new log connection manager.
     */
    public LogConnectionManager() throws THLException
    {
        nextTimeoutCheck = lastTimeoutCheck + timeoutMillis;
    }

    /**
     * Returns the number of connections currently managed.
     */
    public synchronized int getSize()
    {
        return connections.size();
    }

    /**
     * Returns the timeout for idle connections.
     */
    public int getTimeoutMillis()
    {
        return timeoutMillis;
    }

    /**
     * Sets the timeout for idle connections. This recomputes the timeout.
     */
    public void setTimeoutMillis(int timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
        nextTimeoutCheck = lastTimeoutCheck + timeoutMillis;
    }

    /**
     * Return a connection to the log file belonging to this client. If the next
     * seqno is before the current position, we release the existing file and
     * return null.
     * 
     * @return A connection. This must be returned using returnLogConnection().
     */
    public synchronized LogConnection getLogConnection(long nextSeqno)
            throws THLException
    {
        long threadId = Thread.currentThread().getId();
        assertNotDone(threadId);
        if (logger.isDebugEnabled())
        {
            logger.debug("Seeking connection to disk log: threadId=" + threadId
                    + " requested seqno=" + nextSeqno);
        }

        // Check for and remove idle connections. This may include the
        // connection we are seeking but doing it this way makes unit tests
        // easier to write.
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > nextTimeoutCheck)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Checking for old connections to free");
            }
            // Compute lower bound for last access time.
            long oldestAccessMillis = currentTimeMillis - timeoutMillis;

            // Use two loops to avoid concurrent access error.
            ArrayList<Long> threadIds = new ArrayList<Long>();
            for (long id : connections.keySet())
            {
                LogConnection lc = connections.get(id);
                if (!lc.isLoaned()
                        && lc.getLastAccessMillis() < oldestAccessMillis)
                {
                    threadIds.add(id);
                }
            }
            for (long id : threadIds)
            {
                releaseConnection(id);
            }

            // Set next timeout check.
            nextTimeoutCheck = currentTimeMillis + timeoutMillis;
        }

        // Look up the connection.
        LogConnection logConnection = connections.get(threadId);
        if (logConnection != null)
        {
            // Assert that this connection was not already loaned.
            if (logConnection.isLoaned())
            {
                throw new THLException(
                        "Log connection already loaned to thread: name="
                                + Thread.currentThread().getName() + " id="
                                + threadId);
            }

            // See if log connection is positioned before requested sequence
            // number.
            if (logConnection.getLastSeqno() <= nextSeqno)
            {
                // Connection is usable, return it.
                logConnection.setLastSeqno(nextSeqno);
                logConnection.setLoaned(true);
                logConnection.setLastAccessMillis(currentTimeMillis);
                return logConnection;
            }
            else
            {
                if (logger.isDebugEnabled())
                {
                    logger
                            .debug("Requested next sequence number is less than connection seqno: requested seqno="
                                    + nextSeqno
                                    + " last seqno="
                                    + logConnection.getLastSeqno());
                }
                connections.remove(threadId);
                logConnection.release();
                return null;
            }
        }
        else
            return null;
    }

    /**
     * Sets the log file and last accessed sequence number for this thread. This
     * allows clients to roll over the log connection and receive it back as a
     * new loaned connection.
     * 
     * @return A connection. This must be returned using returnLogConnection().
     */
    public synchronized LogConnection createAndGetLogConnection(
            LogFile logFile, long lastSeqno) throws THLException
    {
        long threadId = Thread.currentThread().getId();
        assertNotDone(threadId);

        // Clear previous connection if any for this thread.
        LogConnection logConnection = connections.remove(threadId);
        if (logConnection != null)
            logConnection.release();

        // Add new connection for this thread.
        logConnection = new LogConnection(logFile, lastSeqno);
        logConnection.setLoaned(true);
        connections.put(threadId, logConnection);

        if (logger.isDebugEnabled())
        {
            logger.debug("Creating new log connection: threadId=" + threadId
                    + " file=" + logFile.getFile().getName() + " seqno="
                    + lastSeqno);
        }
        return logConnection;
    }

    /**
     * Returns a loaned log connection. This must be called on any connection
     * before asking for it again.
     * 
     * @param logConnection
     */
    public synchronized void returnLogConnection(LogConnection logConnection)
    {
        logConnection.setLoaned(false);
    }

    /**
     * Releases all connections. This must be called when terminating to ensure
     * file descriptors are released.
     */
    public synchronized void release()
    {
        done = true;

        // Use two loops to avoid concurrent access error.
        ArrayList<Long> threadIds = new ArrayList<Long>(connections.size());
        for (long threadId : connections.keySet())
        {
            threadIds.add(threadId);
        }
        for (long threadId : threadIds)
        {
            releaseConnection(threadId);
        }

        connections = null;
    }

    // Ensure that log is still accessible.
    private void assertNotDone(long threadId) throws THLException
    {
        if (done)
        {
            throw new THLException("Illegal access on closed log: threadId="
                    + threadId);
        }
    }

    /**
     * Releases log connection for current thread.
     */
    public void releaseConnection()
    {
        releaseConnection(Thread.currentThread().getId());
    }

    /**
     * Releases log connection for specific thread ID.
     */
    public synchronized void releaseConnection(long threadId)
    {
        LogConnection logConnection = connections.get(threadId);
        if (logConnection != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Releasing log connection: file="
                        + logConnection.getLogFile().getFile().getName()
                        + " threadid=" + threadId);
            }
            logConnection.release();
            connections.remove(threadId);
        }
    }
}