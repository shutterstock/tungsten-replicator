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
    private static final Logger             logger                  = Logger.getLogger(THLParallelReadQueue.class);

    // Totally ordered event queue from read task.
    private final BlockingQueue<ReplEvent>  eventQueue;

    // Pending control events to be integrated into the event queue and seqno
    // of next event if known.
    private BlockingQueue<ReplControlEvent> controlQueue;

    // Counters to track when to merge control events.
    private long                            readSeqno               = 0;
    private boolean                         inFragmentedTransaction = false;

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

    /**
     * Post a control event, which will either be immediately added to the event
     * queue or buffered in the control event queue until it is time to merge
     * it.
     * 
     * @param controlEvent Control event to post or buffer
     */
    public synchronized void post(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        // If the seqno >= controlEvent seqno, enqueue now.
        if (controlEvent.getSeqno() <= readSeqno
                && !this.inFragmentedTransaction)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Enqueuing control event to parallel queue:  seqno="
                        + controlEvent.getSeqno()
                        + " type="
                        + controlEvent.getEventType());
            }
            eventQueue.put(controlEvent);
        }
        else
        {
            // TODO: Potential deadlock!
            if (logger.isDebugEnabled())
            {
                logger.debug("Buffering control event for future handling:  seqno="
                        + controlEvent.getSeqno()
                        + " type="
                        + controlEvent.getEventType());
            }
            this.controlQueue.put(controlEvent);
        }
    }

    /**
     * Post a transaction event which will be enqueued immediately. Synchronize
     * position to update counters and catch any pending control event as well.
     * 
     * @param Replication event to post
     */
    public void post(ReplDBMSEvent replDBMSEvent)
            throws InterruptedException
    {
        // Post the event.
        if (logger.isDebugEnabled())
        {
            logger.debug("Adding event to parallel queue:  seqno="
                    + replDBMSEvent.getSeqno() + " fragno="
                    + replDBMSEvent.getFragno());
        }
        eventQueue.put(replDBMSEvent);

        // Synchronize.
        sync(replDBMSEvent.getSeqno(), replDBMSEvent.getLastFrag());
    }

    /**
     * Synchronize the position pointers and enqueue any pending control event
     * that is now ready to go.
     * 
     * @param seqno Current sequence number
     * @param lastFrag Is the transaction the last fragment
     */
    public synchronized void sync(long seqno, boolean lastFrag)
            throws InterruptedException
    {
        // Update position counters.
        readSeqno = seqno;
        inFragmentedTransaction = !lastFrag;

        // If there is a pending controlEvent and we are at the end
        // of the transaction, post the event now.
        ReplControlEvent controlEvent = controlQueue.peek();
        if (controlEvent != null && controlEvent.getSeqno() <= readSeqno
                && !this.inFragmentedTransaction)
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