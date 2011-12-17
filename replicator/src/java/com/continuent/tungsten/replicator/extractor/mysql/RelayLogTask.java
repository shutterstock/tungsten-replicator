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

package com.continuent.tungsten.replicator.extractor.mysql;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.extractor.ExtractorException;

/**
 * Implements a task that runs in a separate thread to manage relay logs.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class RelayLogTask implements Runnable
{
    private static Logger    logger    = Logger.getLogger(RelayLogTask.class);
    private RelayLogClient   relayClient;
    private volatile boolean cancelled = false;
    private volatile boolean finished  = false;

    /**
     * Creates a new relay log task.
     */
    public RelayLogTask(RelayLogClient relayClient)
    {
        this.relayClient = relayClient;
    }

    /**
     * Extracts from the relay log until cancelled or we fail.
     */
    public void run()
    {
        logger.info("Relay log task starting: "
                + Thread.currentThread().getName());
        try
        {
            while (!cancelled && !Thread.currentThread().isInterrupted())
            {
                if (!relayClient.processEvent())
                {
                    if (cancelled)
                    {
                        logger.info("Event processing was cancelled. Returning without processing event.");
                        return;
                    }
                    else
                    {
                        throw new ExtractorException(
                                "Network download of binlog failed; may indicated that MySQL terminated the connection.  Check your serverID setting!");
                    }

                }
            }
        }
        catch (InterruptedException e)
        {
            logger.info("Relay log task cancelled by interrupt");
        }
        catch (Throwable t)
        {
            logger.error(
                    "Relay log task failed due to exception: " + t.getMessage(),
                    t);
        }
        finally
        {
            relayClient.disconnect();
        }

        logger.info("Relay log task ending: "
                + Thread.currentThread().getName());
        finished = true;
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

    /**
     * Returns the current relay log position.
     */
    public RelayLogPosition getPosition()
    {
        return relayClient.getPosition();
    }
}