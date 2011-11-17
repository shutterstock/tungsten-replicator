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

import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.patterns.event.EventDispatcher;
import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.parallel.Partitioner;
import com.continuent.tungsten.replicator.storage.parallel.PartitionerResponse;
import com.continuent.tungsten.replicator.thl.log.LogConnection;
import com.continuent.tungsten.replicator.thl.log.LogEventReadFilter;
import com.continuent.tungsten.replicator.thl.log.LogEventReplReader;
import com.continuent.tungsten.replicator.util.AtomicCounter;
import com.continuent.tungsten.replicator.util.AtomicIntervalGuard;

/**
 * Performs coordinated reads on the THL on behalf of a particular client (a
 * task thread) and buffers log records up to a local limit.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelReadTask implements Runnable
{
    private static Logger                  logger               = Logger.getLogger(THLParallelReadTask.class);

    // Task number on whose behalf we are reading.
    private final int                      taskId;

    // Partitioner instance.
    private final Partitioner              partitioner;

    // Counters to coordinate queue operation.
    private AtomicCounter                  headSeqnoCounter;
    private AtomicIntervalGuard<?>         intervalGuard;
    private AtomicLong                     lowWaterMark         = new AtomicLong(
                                                                        0);
    private AtomicLong                     acceptCount          = new AtomicLong(
                                                                        0);
    private AtomicLong                     discardCount         = new AtomicLong(
                                                                        0);
    private AtomicLong                     readCount            = new AtomicLong(
                                                                        0);

    // Dispatcher to report errors.
    EventDispatcher                        dispatcher;

    // Ordered queue of events for clients.
    private final BlockingQueue<ReplEvent> eventQueue;

    // Number of transactions between automatic sync events.
    private final int                      syncInterval;

    // Queue parameters.
    private final int                      maxControlEvents;
    private long                           restartSeqno         = 0;
    private long                           restartExtractMillis = Long.MAX_VALUE;

    // Pending control events to be integrated into the event queue and seqno
    // of next event if known.
    private THLParallelReadQueue           readQueue;

    // Connection to the log.
    private THL                            thl;
    private LogConnection                  connection;

    // Throwable trapped from run loop.
    private volatile Throwable             throwable;

    // Thread ID for this read task.
    private volatile Thread                taskThread;

    // Flag indicating task is cancelled.
    private volatile boolean               cancelled            = false;

    /**
     * Instantiate a read task.
     */
    public THLParallelReadTask(int taskId, THL thl, Partitioner partitioner,
            AtomicCounter headSeqnoCounter,
            AtomicIntervalGuard<?> intervalGuard, int maxSize,
            int maxControlEvents, int syncInterval, EventDispatcher dispatcher)
    {
        this.taskId = taskId;
        this.thl = thl;
        this.partitioner = partitioner;
        this.headSeqnoCounter = headSeqnoCounter;
        this.intervalGuard = intervalGuard;
        this.maxControlEvents = maxControlEvents;
        this.eventQueue = new LinkedBlockingQueue<ReplEvent>(maxSize);
        this.syncInterval = syncInterval;
        this.dispatcher = dispatcher;
    }

    /**
     * Set the starting header. This must be called before prepare().
     */
    public synchronized void setRestartHeader(ReplDBMSHeader header)
    {
        this.restartSeqno = header.getSeqno() + 1;
        this.restartExtractMillis = header.getExtractedTstamp().getTime();
    }

    /**
     * Connect to THL and seek start sequence number. Must be called before
     * run().
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Set up the read queue.
        this.readQueue = new THLParallelReadQueue(eventQueue, maxControlEvents,
                restartSeqno);

        // Connect to the log.
        connection = thl.connect(true);

        // Add a read filter that will accept only events that are in this
        // partition. We use an inner class so we can access the partitioner
        // and task id easily.
        LogEventReadFilter filter = new LogEventReadFilter()
        {
            public boolean accept(LogEventReplReader reader)
                    throws ReplicatorException
            {
                ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                        reader.getSeqno(), reader.getFragno(),
                        reader.isLastFrag(), reader.getSourceId(),
                        reader.getEpochNumber(), reader.getEventId(),
                        reader.getShardId(), new Timestamp(
                                reader.getSourceTStamp()));
                PartitionerResponse response;
                try
                {
                    response = partitioner.partition(header, taskId);
                }
                catch (THLException e)
                {
                    throw e;
                }
                catch (ReplicatorException e)
                {
                    throw new THLException(e.getMessage(), e);
                }
                return (taskId == response.getPartition());
            }
        };
        connection.setReadFilter(filter);

        // Report our starting position to the interval guard.
        intervalGuard.report(taskId, restartSeqno, restartExtractMillis);
    }

    /**
     * Start the task thread. This must be called after prepare.
     */
    public synchronized void start()
    {
        if (this.taskThread == null)
        {
            taskThread = new Thread(this);
            taskThread.setName("store-" + thl.getName() + "-" + taskId);
            taskThread.start();
        }
    }

    /**
     * Cancel the thread. This must be called prior to release.
     */
    public synchronized void stop()
    {
        cancelled = true;
        if (this.taskThread != null)
        {
            taskThread.interrupt();
            try
            {
                taskThread.join(2000);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Terminate reader task and free all resources. Must be called following
     * run().
     */
    public synchronized void release()
    {
        if (connection != null)
        {
            connection.release();
            connection = null;
            eventQueue.clear();
            readQueue.release();
        }
    }

    /**
     * Implements read loop on the log to feed event queue.
     */
    @Override
    public void run()
    {
        // Get the starting sequence number.
        long readSeqno = restartSeqno;

        try
        {
            // Seek to initial position to start reading.
            if (!connection.seek(restartSeqno))
            {
                throw new THLException(
                        "Unable to locate starting seqno in log: seqno="
                                + restartSeqno + " store=" + thl.getName()
                                + " taskId=" + taskId);
            }

            // Read records until we are cancelled.
            while (!cancelled)
            {
                // Read next event from the log.
                THLEvent thlEvent = connection.next();
                readSeqno = thlEvent.getSeqno();
                if (lowWaterMark.get() == 0)
                    lowWaterMark.set(readSeqno);
                readCount.incrementAndGet();
                if (logger.isDebugEnabled())
                {
                    logger.debug("Read event from THL: seqno="
                            + thlEvent.getSeqno() + " fragno="
                            + thlEvent.getFragno() + " lastFrag="
                            + thlEvent.getLastFrag() + " deserialized="
                            + (thlEvent.getReplEvent() != null));
                }

                // Ensure it is safe to process this value. This lock prevents
                // our thread from jumping too far ahead of others and
                // coordinates serialization.
                headSeqnoCounter.waitSeqnoGreaterEqual(thlEvent.getSeqno());

                // Report our position to the interval guard.
                intervalGuard.report(taskId, thlEvent.getSeqno(), thlEvent
                        .getSourceTstamp().getTime());

                // If we do not want it, just go to the next event. This
                // would be null if the read filter discarded the event due
                // to it being in another partition.
                ReplDBMSEvent replDBMSEvent = (ReplDBMSEvent) thlEvent
                        .getReplEvent();
                if (replDBMSEvent == null)
                {
                    discardCount.incrementAndGet();
                    checkSync(thlEvent);
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Discarded null event: seqno="
                                + thlEvent.getSeqno() + " fragno="
                                + thlEvent.getFragno());
                    }
                    continue;
                }

                // Discard empty events. These should not be common.
                DBMSEvent dbmsEvent = replDBMSEvent.getDBMSEvent();
                if (dbmsEvent == null | dbmsEvent instanceof DBMSEmptyEvent
                        || dbmsEvent.getData().size() == 0)
                {
                    discardCount.incrementAndGet();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Discarded empty event: seqno="
                                + thlEvent.getSeqno() + " fragno="
                                + thlEvent.getFragno());
                    }
                    checkSync(thlEvent);
                    continue;
                }

                // Add to queue.
                if (logger.isDebugEnabled())
                {
                    logger.debug("Adding event to parallel queue:  taskId="
                            + taskId + " seqno=" + replDBMSEvent.getSeqno()
                            + " fragno=" + replDBMSEvent.getFragno());
                }
                acceptCount.incrementAndGet();
                readQueue.post(replDBMSEvent);
            }
        }
        catch (InterruptedException e)
        {
            if (!cancelled)
                logger.warn("Unexpected interrupt before reader thread was cancelled");
        }
        catch (Throwable e)
        {
            String msg = "Read failed on transaction log: seqno=" + readSeqno
                    + " taskId=" + taskId;
            try
            {
                dispatcher.put(new ErrorNotification(msg, e));
            }
            catch (InterruptedException e1)
            {
                logger.warn("Task cancelled while posting error notification",
                        null);
            }

            throwable = e;
        }

        // Close up shop.
        logger.info("Terminating parallel reader thread: seqno=" + readSeqno
                + " store=" + thl.getName() + " taskId=" + taskId);
    }

    // Handle synchronization of the read position in the queue.
    private void checkSync(THLEvent thlEvent) throws InterruptedException
    {
        long seqno = thlEvent.getSeqno();
        // See if we are over the synchronization interval.
        if (thlEvent.getLastFrag() && seqno % syncInterval == 0)
        {
            // If so, submit a synchronization event.
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(
                    thlEvent.getSeqno(), thlEvent.getFragno(),
                    thlEvent.getLastFrag(), thlEvent.getSourceId(),
                    thlEvent.getEpochNumber(), thlEvent.getEventId(),
                    thlEvent.getShardId(), thlEvent.getSourceTstamp());
            ReplControlEvent ctrl = new ReplControlEvent(ReplControlEvent.SYNC,
                    seqno, header);

            if (logger.isDebugEnabled())
            {
                logger.debug("Inserting sync event: seqno=" + seqno);
            }
            readQueue.post(ctrl);
        }
        else
        {
            // Otherwise, just update the read position.
            readQueue.sync(thlEvent.getSeqno(), thlEvent.getLastFrag());
        }
    }

    // QUEUE INTERFACE STARTS HERE.

    /**
     * Returns the current queue size.
     */
    public int size()
    {
        return eventQueue.size();
    }

    /**
     * Removes and returns next event from the queue, blocking if empty. This
     * call blocks if no event is available.
     * 
     * @return The next event in the queue
     * @throws InterruptedException Thrown if method is interrupted
     * @throws ReplicatorException Thrown if the reader thread has failed
     */
    public ReplEvent get() throws InterruptedException, ReplicatorException
    {
        // Check for read thread liveness.
        if (throwable != null)
        {
            // If this happens the thread has died.
            throw new ReplicatorException("THL reader thread failed", throwable);
        }
        else if (cancelled)
        {
            // If this is true the thread has been cancelled. This should never
            // occur before the caller thread has exited.
            throw new ReplicatorException("THL reader thread is cancelled");
        }

        // Get the next event and return it.
        ReplEvent event = eventQueue.take();
        if (logger.isDebugEnabled())
        {
            logger.debug("Returning event from queue: seqno="
                    + event.getSeqno() + " type="
                    + event.getClass().getSimpleName() + " taskId=" + taskId
                    + " activeSize=" + size());
        }
        return event;
    }

    /**
     * Returns but does not remove next event from the queue if it exists or
     * returns null if queue is empty.
     */
    public ReplEvent peek() throws InterruptedException
    {
        return eventQueue.peek();
    }

    /**
     * Inserts a control event.
     */
    public void putControlEvent(ReplControlEvent controlEvent)
            throws InterruptedException
    {
        readQueue.postOutOfBand(controlEvent);
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" task_id=").append(taskId);
        sb.append(" thread_name=");
        if (taskThread == null)
            sb.append("null");
        else
            sb.append(taskThread.getName());
        sb.append(" hi_seqno=").append(this.readQueue.getReadSeqno());
        sb.append(" lo_seqno=").append(lowWaterMark.get());
        sb.append(" read=").append(readCount);
        sb.append(" accepted=").append(acceptCount);
        sb.append(" discarded=").append(discardCount);
        sb.append(" events=").append(eventQueue.size());
        return sb.toString();
    }
}