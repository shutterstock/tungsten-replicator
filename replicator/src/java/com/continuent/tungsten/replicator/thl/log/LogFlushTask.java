/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class implements a task to issue asynchronous flush calls on active log
 * files.
 * <p>
 * Important concurrency note: This class calls back into LogFile instances,
 * which in turn call this class to register themselves. Methods on this class
 * should *not* be synchronized as this would create the possibility of
 * deadlock. Synchronization within this class is handled by the
 * ConncurrentHashMap that contains the log files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogFlushTask implements Runnable
{
    private static Logger      logger     = Logger
                                                  .getLogger(LogFlushTask.class);
    private volatile boolean   cancelled  = false;
    private volatile boolean   finished   = false;

    private long               flushIntervalMillis;

    private Map<File, LogFile> logFileMap = new ConcurrentHashMap<File, LogFile>();

    /**
     * Creates a new log sync task.
     */
    public LogFlushTask(long flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
    }

    /**
     * Extracts from the relay log until cancelled or we fail.
     */
    public void run()
    {
        logger.info("Log sync task starting: "
                + Thread.currentThread().getName());

        try
        {
            while (!cancelled && !Thread.currentThread().isInterrupted())
            {
                Thread.sleep(flushIntervalMillis);
                processSync();
            }
        }
        catch (InterruptedException e)
        {
            logger.info("Log sync task cancelled by interrupt");
        }
        catch (Throwable t)
        {
            logger.error("Log sync task failed due to exception: "
                    + t.getMessage(), t);
        }

        logger
                .info("Log sync task ending: "
                        + Thread.currentThread().getName());
        finished = true;
    }

    /**
     * Issue a synchronization call.
     */
    private void processSync() throws ReplicatorException, IOException,
            InterruptedException
    {
        Collection<LogFile> logFiles = logFileMap.values();
        for (LogFile logFile : logFiles)
        {
            logFile.flush();
        }
    }

    /**
     * Adds a logFile to the list for regular synchronization.
     * 
     * @param logFile
     */
    public void addLogFile(LogFile logFile)
    {
        this.logFileMap.put(logFile.getFile(), logFile);
    }

    /**
     * Removes a logfile from the list.
     * 
     * @param logFile
     */
    public void removeLogFile(LogFile logFile)
    {
        logFileMap.remove(logFile.getFile());
    }

    /**
     * Signal that the task should end.
     */
    public void cancel()
    {
        cancelled = true;
    }

    /**
     * Returns true if the task has completed.
     */
    public boolean isFinished()
    {
        return finished;
    }
}