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

package com.continuent.tungsten.replicator.thl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;

/**
 * Implements queue control logic for a single parallel read task. This class
 * merges regular events (i.e., transactions and transaction fragments) with
 * control events to create a totally ordered event queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelReadQueue
{
    private static final Logger             logger    = Logger.getLogger(THLParallelReadQueue.class);

    // Totally ordered event queue from read task.
    private final BlockingQueue<ReplEvent>  eventQueue;

    // Pending control events to be integrated into the event queue and seqno
    // of next event if known.
    private BlockingQueue<ReplControlEvent> controlQueue;

    // Counters to track when to merge control events. These are declared
    // volatile to permit non-blocking reads.
    private volatile long                   readSeqno = 0;
    private volatile boolean                lastFrag  = true;

    /**
     * Instantiates a new read queue.
     * 
     * @param eventQueue Queue into which we feed events
     * @param maxControlEvents Maximum number of control events to buffer
     * @param startingSeqno Starting sequence number (-1 for initialization)
     */
    public THLParallelReadQueue(BlockingQueue<ReplEvent> eventQueue,
            int maxControlEvents, long startingSeqno)
    {
        this.eventQueue = eventQueue;
        this.controlQueue = new LinkedBlockingQueue<ReplControlEvent>(
                maxControlEvents);
        this.readSeqno = startingSeqno;
    }

    /** Returns current sequence number we have read. */
    public long getReadSeqno()
    {
        return readSeqno;
    }

    /** Returns whether last event read was the end of a transaction. */
    public boolean isLastFrag()
    {
        return lastFrag;
    }

    /**
     * Post a control event, which will either be immediately added to the event
     * queue or buffered in the control event queue until it is time to merge
     * it.
     * 
     * @param controlEvent Control event to post or buffer
     */
    public synchronized void postOutOfBand(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        // If the seqno >= controlEvent seqno, enqueue now.
        if (controlEvent.getSeqno() <= readSeqno && lastFrag)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Enqueuing control event to parallel queue: seqno="
                        + controlEvent.getSeqno()
                        + " type="
                        + controlEvent.getEventType()
                        + " readSeqno="
                        + readSeqno + " lastFrag=" + lastFrag);
            }
            eventQueue.put(controlEvent);
        }
        else
        {
            // TODO: Potential deadlock if the controlQueue is full!
            if (logger.isDebugEnabled())
            {
                logger.debug("Buffering control event for future handling:  seqno="
                        + controlEvent.getSeqno()
                        + " type="
                        + controlEvent.getEventType()
                        + " readSeqno="
                        + readSeqno + " lastFrag=" + lastFrag);
            }
            controlQueue.put(controlEvent);
        }
    }

    /**
     * Post an event which will be enqueued immediately. Synchronize position to
     * update counters and catch any pending control event as well.
     * 
     * @param Replication event to post. Must either be proper event or a
     *            control event for synchronization.
     */
    public synchronized void post(ReplEvent replEvent)
            throws InterruptedException
    {
        // Update position using header information from event.
        ReplDBMSHeader header;
        if (replEvent instanceof ReplDBMSEvent)
            header = (ReplDBMSEvent) replEvent;
        else
            header = ((ReplControlEvent) replEvent).getHeader();

        // Post the event we received.
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding event to parallel queue:  seqno="
                    + replEvent.getSeqno());
        }
        eventQueue.put(replEvent);

        // Sync our position.
        sync(header.getSeqno(), header.getLastFrag());
    }

    /**
     * Synchronizes the current position of the queue by storing the sequence
     * number and whether we are in a fragmented transaction. Automatically
     * posts any pending control events, if these are present. This method must
     * be called whenever we read even if there is no event to post so that
     * control events are correctly folded into event stream fed to clients.
     */
    public synchronized void sync(long seqno, boolean lastFrag)
            throws InterruptedException
    {
        // Update our position.
        this.readSeqno = seqno;
        this.lastFrag = lastFrag;

        // If there is a pending out-of-band control event and we are at the end
        // of the transaction, post that event now.
        ReplControlEvent controlEvent = controlQueue.peek();
        if (controlEvent != null && controlEvent.getSeqno() <= readSeqno
                && lastFrag)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Dequeueing buffered control event and enqueueing to parallel queue:  seqno="
                        + controlEvent.getSeqno()
                        + " type="
                        + controlEvent.getEventType());
            }
            controlEvent = controlQueue.take();
            eventQueue.put(controlEvent);
        }
    }

    /**
     * Frees resources, which in this case is just the control queue as we do
     * not own the event queue.
     */
    public synchronized void release()
    {
        if (controlQueue != null)
        {
            controlQueue = null;
        }
    }
}