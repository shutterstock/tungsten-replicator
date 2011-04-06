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

/**
 * Maintains a cursor on the log for a particular client. The last sequence
 * number is effectively a cursor to ensure that clients do not rewrite the log.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogConnection
{
    private LogFile logFile;
    private long    lastSeqno;
    private long    lastAccessMillis;
    private boolean loaned;

    /**
     * Create a new log connection.
     */
    public LogConnection(LogFile logFile, long lastSeqno)
    {
        this.logFile = logFile;
        this.lastSeqno = lastSeqno;
        this.lastAccessMillis = System.currentTimeMillis();
    }

    public LogFile getLogFile()
    {
        return logFile;
    }

    public long getLastSeqno()
    {
        return lastSeqno;
    }

    public void setLastSeqno(long lastSeqno)
    {
        this.lastSeqno = lastSeqno;
    }

    public long getLastAccessMillis()
    {
        return lastAccessMillis;
    }

    public void setLastAccessMillis(long lastAccessMillis)
    {
        this.lastAccessMillis = lastAccessMillis;
    }

    public boolean isLoaned()
    {
        return loaned;
    }

    public void setLoaned(boolean loaned)
    {
        this.loaned = loaned;
    }

    /**
     * Releases underlying log file.
     */
    public void release()
    {
        logFile.release();
        logFile = null;
    }
}