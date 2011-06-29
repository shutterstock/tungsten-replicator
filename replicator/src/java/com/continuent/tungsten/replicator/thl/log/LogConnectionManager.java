/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Encapsulates management of connections and their log cursors. Log cursors are
 * a position in a particular log file and can only move in a forward direction.
 * If clients move backward in the log we need to allocated a new cursor.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogConnectionManager
{
    private static Logger       logger          = Logger.getLogger(LogConnectionManager.class);

    // Map of active cursors and flag to indicate we are closed for
    // business.
    private boolean             done;

    // Connection pools.
    private LogConnection       writeConnection;
    private List<LogConnection> readConnections = new ArrayList<LogConnection>();

    /**
     * Create a new log cursor manager.
     */
    public LogConnectionManager()
    {
    }

    /**
     * Stores a new connection.
     */
    public synchronized void store(LogConnection connection)
            throws THLException
    {
        // Ensure we are still open for business.
        assertNotDone(connection);

        // Clean up all finished connections.
        if (writeConnection != null && writeConnection.isDone())
            writeConnection = null;
        int readConnectionsSize = readConnections.size();
        for (int i = 0; i < readConnectionsSize; i++)
        {
            // Have to walk backwards through the read connections.
            int index = readConnectionsSize - i - 1;
            if (readConnections.get(index).isDone())
                readConnections.remove(index);
        }

        // To prevent chaos only a single write connection is allowed.
        if (!connection.isReadonly() && writeConnection != null)
            throw new THLException(
                    "Write connection already exists: connection="
                            + writeConnection.toString());

        // Allocate, store, and return the connection.
        if (connection.isReadonly())
            readConnections.add(connection);
        else
            writeConnection = connection;

    }

    /**
     * Releases an existing connection.
     */
    public synchronized void release(LogConnection connection)
    {
        // Warn if we are shut down.
        if (done)
        {
            logger.warn("Attempt to release connection after connection manager shutdown: "
                    + connection);
            return;
        }

        // Release the connection.
        connection.releaseInternal();
        if (connection.isReadonly())
        {
            if (!readConnections.remove(connection))
                logger.warn("Unable to free read-only connection: "
                        + connection);
        }
        else
        {
            if (writeConnection == connection)
                writeConnection = null;
            else
            {
                logger.warn("Unable to free write connection: " + connection);
            }
        }
    }

    /**
     * Releases all connections. This must be called when terminating to ensure
     * file descriptors are released.
     */
    public synchronized void releaseAll()
    {
        if (!done)
        {
            // Release all connections.
            if (this.writeConnection != null)
            {
                // This flushes pending output.
                writeConnection.releaseInternal();
                writeConnection = null;
            }
            for (LogConnection connection : readConnections)
            {
                connection.releaseInternal();
            }
            readConnections = null;

            done = true;
        }
    }

    // Ensure that log is still accessible.
    private void assertNotDone(LogConnection client) throws THLException
    {
        if (done)
        {
            throw new THLException("Illegal access on closed log: client="
                    + client);
        }
    }
}