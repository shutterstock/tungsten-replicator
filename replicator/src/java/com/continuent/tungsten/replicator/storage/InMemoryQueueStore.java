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

package com.continuent.tungsten.replicator.storage;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements an in-memory event store. This queue has no memory beyond its
 * current contents.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class InMemoryQueueStore implements Store
{
    private static Logger                      logger           = Logger.getLogger(InMemoryQueueStore.class);
    private String                             name;
    private LinkedBlockingQueue<ReplDBMSEvent> queue;
    private int                                maxSize          = 1;
    private ReplDBMSHeader                     lastHeader;
    private long                               transactionCount = 0;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getMaxSize()
    {
        return maxSize;
    }

    public void setMaxSize(int size)
    {
        this.maxSize = size;
    }

    /** Sets the last header processed. This is required for restart. */
    public void setLastHeader(ReplDBMSHeader header)
    {
        lastHeader = header;
    }

    /** Returns the last header processed. */
    public ReplDBMSHeader getLastHeader()
    {
        return lastHeader;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.storage.Store#getMaxStoredSeqno(boolean)
     */
    public long getMaxStoredSeqno(boolean adminCommand)
    {
        return 0;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.storage.Store#getMinStoredSeqno(boolean)
     */
    public long getMinStoredSeqno(boolean adminCommand)
    {
        return 0;
    }

    /**
     * As in-memory queue don't keep events, this function is no-op and just
     * returns null
     *
     * @return null
     */
    public ReplDBMSEvent fetchEvent(long seqno, short fragno,
            boolean ignoreSkippedEvent)
    {
        // We don't keep events persistently, so this is always null.
        return null;
    }

    /**
     * Puts an event in the queue, blocking if it is full.
     */
    public void put(ReplDBMSEvent event) throws InterruptedException
    {
        queue.put(event);
        transactionCount++;
        if (logger.isDebugEnabled())
        {
            if (transactionCount % 10000 == 0)
                logger.debug("Queue store: xacts=" + transactionCount
                        + " size=" + queue.size());
        }
    }

    /**
     * Removes and returns next event from the queue, blocking if empty.
     */
    public ReplDBMSEvent get() throws InterruptedException
    {
        return queue.take();
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplDBMSEvent peek()
    {
        return queue.peek();
    }

    /**
     * Returns the current queue size.
     */
    public int size()
    {
        return queue.size();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // Nothing to do.
    }

    /**
     * Allocate an in-memory queue. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        queue = new LinkedBlockingQueue<ReplDBMSEvent>(maxSize);
    }

    /**
     * Release queue. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        queue = null;
    }

    @Override
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        if (queue != null)
            props.setLong("storeSize", queue.size());
        else
            props.setLong("storeSize", -1);
        props.setLong("maxSize", maxSize);
        props.setLong("eventCount", this.transactionCount);
        return props;
    }
}
