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

package com.continuent.tungsten.replicator.storage.parallel;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.ParallelExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements ParallelExtractor interface for a parallel queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */

public class ParallelQueueExtractor implements ParallelExtractor
{
    private static Logger      logger = Logger.getLogger(ParallelQueueExtractor.class);

    private int                taskId = -1;
    private String             storeName;
    private ParallelQueueStore parallelQueue;

    /**
     * Instantiate the adapter.
     */
    public ParallelQueueExtractor()
    {
    }

    public String getStoreName()
    {
        return storeName;
    }

    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#extract()
     */
    public ReplEvent extract() throws ExtractorException, InterruptedException
    {
        try
        {
            return parallelQueue.get(taskId);
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException(
                    "Unable to extract event from parallel queue: name="
                            + storeName, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ExtractorException,
            InterruptedException
    {
        try
        {
            ReplEvent event = parallelQueue.peek(taskId);
            if (event == null)
                return null;
            else if (event instanceof ReplDBMSEvent)
                return ((ReplDBMSEvent) event).getEventId();
            else if (event instanceof ReplControlEvent)
            {
                ReplDBMSHeader event2 = ((ReplControlEvent) event).getHeader();
                if (event2 == null)
                    return null;
                else
                    return event2.getEventId();
            }
            else
            {
                // This should not happen.
                logger.warn("Returned unexpected event type from peek operation: "
                        + event.getClass().toString());
                return null;
            }
        }
        catch (ReplicatorException e)
        {
            throw new ExtractorException(
                    "Unable to extract event from parallel queue: name="
                            + storeName, e);
        }
    }

    /**
     * Returns true if the queue has more events. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#hasMoreEvents()
     */
    public boolean hasMoreEvents()
    {
        return parallelQueue.size(taskId) > 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
    }

    /**
     * Connect to underlying queue. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        try
        {
            parallelQueue = (ParallelQueueStore) context.getStore(storeName);
        }
        catch (ClassCastException e)
        {
            throw new ReplicatorException(
                    "Invalid storage class; configuration may be in error: "
                            + context.getStore(storeName).getClass().getName());
        }
        if (parallelQueue == null)
            throw new ReplicatorException(
                    "Unknown storage name; configuration may be in error: "
                            + storeName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        parallelQueue = null;
    }

    /**
     * Return the header, which should have been place here by an extractor
     * during restart.
     * 
     * @see com.continuent.tungsten.replicator.applier.Applier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return parallelQueue.getLastHeader(taskId);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.ParallelApplier#setTaskId(int)
     */
    public void setTaskId(int id) throws ExtractorException
    {
        this.taskId = id;
    }

    /**
     * Store the header so that it can be propagated back through the pipeline
     * for restart. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEvent(com.continuent.tungsten.replicator.event.ReplDBMSHeader)
     */
    public void setLastEvent(ReplDBMSHeader header) throws ReplicatorException
    {
        parallelQueue.setLastHeader(taskId, header);
    }

    /**
     * Ignored for now as in-memory queues do not extract. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.Extractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        logger.warn("Attempt to set last event ID on queue storage: " + eventId);
    }
}
