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

package com.continuent.tungsten.replicator.pipeline;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.storage.ParallelStore;
import com.continuent.tungsten.replicator.util.EventIdWatchPredicate;
import com.continuent.tungsten.replicator.util.HeartbeatWatchPredicate;
import com.continuent.tungsten.replicator.util.SeqnoWatchPredicate;
import com.continuent.tungsten.replicator.util.SourceTimestampWatchPredicate;
import com.continuent.tungsten.replicator.util.Watch;
import com.continuent.tungsten.replicator.util.WatchAction;
import com.continuent.tungsten.replicator.util.WatchManager;
import com.continuent.tungsten.replicator.util.WatchPredicate;

/**
 * Tracks the current status of replication and implements event watches.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class StageProgressTracker
{
    private static Logger                        logger              = Logger.getLogger(StageProgressTracker.class);
    String                                       name;

    // Record of last processed event on each task.
    private final int                            threadCount;
    private final TaskProgress[]                 taskInfo;

    // Record of last processed info on each shard.
    private final TreeMap<String, ShardProgress> shardInfo           = new TreeMap<String, ShardProgress>();

    // Watch lists.
    private final WatchManager<ReplDBMSHeader>   seqnoWatches        = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader>   eventIdWatches      = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader>   heartbeatWatches    = new WatchManager<ReplDBMSHeader>();
    private final WatchManager<ReplDBMSHeader>   timestampWatches    = new WatchManager<ReplDBMSHeader>();

    // Upstream parallel store for inserting watch events.
    ParallelStore                                upstreamStore       = null;

    // If this is set, the task should be interrupted.
    private boolean                              shouldInterruptTask = false;

    // Watch action to terminate this task.
    WatchAction<ReplDBMSHeader>                  cancelAction        = new WatchAction<ReplDBMSHeader>()
                                                                     {
                                                                         public void matched(
                                                                                 ReplDBMSHeader event,
                                                                                 int taskId)
                                                                         {
                                                                             taskInfo[taskId]
                                                                                     .setCancelled(true);
                                                                         }
                                                                     };

    // Global reporting counters. We also report on individual tasks.
    private long                                 eventCount          = 0;
    private long                                 loggingInterval     = 0;
    private long                                 applyLatencyMillis  = 0;

    // Variables used to skip events.
    private long                                 applySkipCount      = 0;
    private SortedSet<Long>                      seqnosToBeSkipped   = null;

    /**
     * Creates a new stage process tracker.
     * 
     * @param name
     */
    public StageProgressTracker(String name, int threadCount)
    {
        // Set instance variables.
        this.name = name;
        this.threadCount = threadCount;
        this.taskInfo = new TaskProgress[threadCount];

        // Initialize task processing data.
        for (int i = 0; i < taskInfo.length; i++)
            taskInfo[i] = new TaskProgress(name, i);

        if (logger.isDebugEnabled())
        {
            logger.info("Initiating stage process tracker for stage: name="
                    + name + " threadCount=" + threadCount);
        }
    }

    /** Sets the upstream parallel store, if such a thing exists. */
    public void setUpstreamStore(ParallelStore upstreamStore)
    {
        this.upstreamStore = upstreamStore;
    }

    /** Print a log message every time we process this many events. */
    public void setLoggingInterval(long loggingInterval)
    {
        this.loggingInterval = loggingInterval;
    }

    /** Set the number of events to skip after going online. */
    public void setApplySkipCount(long applySkipCount)
    {
        this.applySkipCount = applySkipCount;
    }

    /** Set a list of one or more events to skip. */
    public void setSeqnosToBeSkipped(SortedSet<Long> seqnosToBeSkipped)
    {
        this.seqnosToBeSkipped = seqnosToBeSkipped;
    }

    /**
     * Return last event that we have seen.
     */
    public synchronized ReplDBMSHeader getLastProcessedEvent(int taskId)
    {
        return taskInfo[taskId].getLastEvent();
    }

    /**
     * Return the last processed sequence number or -1 if no event exists. This
     * event is the minimum value that has been reached.
     */
    public synchronized long getMinLastSeqno()
    {
        long minSeqno = Long.MAX_VALUE;
        for (TaskProgress progress : taskInfo)
        {
            ReplDBMSHeader event = progress.getLastEvent();
            if (event == null)
                minSeqno = -1;
            else
                minSeqno = Math.min(minSeqno, event.getSeqno());
        }
        return minSeqno;
    }

    /**
     * Return the last processed event or null if none such exists.
     */
    public synchronized ReplDBMSHeader getMinLastEvent()
    {
        ReplDBMSHeader minEvent = null;
        for (TaskProgress progress : taskInfo)
        {
            ReplDBMSHeader event = progress.getLastEvent();
            if (event == null)
            {
                minEvent = null;
                break;
            }
            else if (minEvent == null || minEvent.getSeqno() > event.getSeqno())
            {
                minEvent = event;
            }
        }
        return minEvent;
    }

    /**
     * Return the current apply latency in milliseconds.
     */
    public synchronized long getApplyLatencyMillis()
    {
        // Latency may be sub-zero due to clock differences.
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    /**
     * Returns a list of cloned task progress instances ordered by task ID.
     */
    public synchronized List<TaskProgress> cloneTaskProgress()
    {
        List<TaskProgress> progressList = new ArrayList<TaskProgress>();
        for (int i = 0; i < threadCount; i++)
            progressList.add(taskInfo[i].clone());
        return progressList;
    }

    /**
     * Return underlying progress instance for a particular task.
     */
    public synchronized TaskProgress getTaskProgress(int taskId)
    {
        return taskInfo[taskId];
    }

    /**
     * Returns a list of shard progress instances ordered by shard ID.
     */
    public synchronized List<ShardProgress> getShardProgress()
    {
        // Get a sorted array of keys and then generate the list.
        List<ShardProgress> progressList = new ArrayList<ShardProgress>();
        for (ShardProgress progress : shardInfo.values())
        {
            progressList.add(progress);
        }
        return progressList;
    }

    /**
     * Set the last processed event, which triggers checks for watches.
     */
    public synchronized void setLastProcessedEvent(int taskId,
            ReplDBMSHeader replEvent) throws InterruptedException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("[" + name + "] setLastProcessedEvent: "
                    + replEvent.getSeqno());
        }
        // Log global statistics.
        eventCount++;
        applyLatencyMillis = System.currentTimeMillis()
                - replEvent.getExtractedTstamp().getTime();

        // Log per-task statistics.
        taskInfo[taskId].incrementEventCount();
        taskInfo[taskId].setApplyLatencyMillis(applyLatencyMillis);

        // Log per-shard statistics.
        String shardId = replEvent.getShardId();
        ShardProgress shardProgress = shardInfo.get(shardId);
        if (shardProgress == null)
        {
            shardProgress = new ShardProgress(shardId,
                    taskInfo[taskId].getStageName());
            shardInfo.put(shardId, shardProgress);
        }
        shardProgress.setLastSeqno(replEvent.getSeqno());
        shardProgress.setLastEventId(replEvent.getEventId());
        shardProgress.incrementEventCount();

        // Log last processed event if greater than stored sequence number.
        if (taskInfo[taskId].getLastEvent() == null
                || taskInfo[taskId].getLastEvent().getSeqno() < replEvent
                        .getSeqno())
        {
            taskInfo[taskId].setLastEvent(replEvent);
        }

        // If we have a real event, process watches.
        if (replEvent instanceof ReplDBMSEvent)
        {
            seqnoWatches.process(replEvent, taskId);
            eventIdWatches.process(replEvent, taskId);
            heartbeatWatches.process(replEvent, taskId);
            timestampWatches.process(replEvent, taskId);
        }
        if (loggingInterval > 0 && eventCount % loggingInterval == 0)
            logger.info("Stage processing counter: event count=" + eventCount);
    }

    /**
     * Signal that task has been cancelled.
     */
    public synchronized void cancel(int taskId)
    {
        taskInfo[taskId].setCancelled(true);
    }

    /**
     * Return true if task has been cancelled.
     */
    public boolean isCancelled(int taskId)
    {
        return taskInfo[taskId].isCancelled();
    }

    /**
     * Signal that all tasks have been cancelled.
     */
    public synchronized void cancelAll()
    {
        for (TaskProgress progress : taskInfo)
            progress.setCancelled(true);
    }

    /**
     * Return true if all task are cancelled.
     */
    public boolean allCancelled()
    {
        for (TaskProgress progress : taskInfo)
        {
            if (progress.isCancelled())
                return false;
        }
        return true;
    }

    /**
     * Return true if we need to interrupt the task(s) after cancellation.
     */
    public boolean shouldInterruptTask()
    {
        return shouldInterruptTask;
    }

    /**
     * Release progress tracker resources.
     */
    public synchronized void release()
    {
        this.eventIdWatches.cancelAll();
        this.seqnoWatches.cancelAll();
    }

    /**
     * Sets a watch for a particular sequence number to be processed.
     * 
     * @param seqno Sequence number to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedSequenceNumber(
            long seqno, boolean cancel) throws InterruptedException
    {
        SeqnoWatchPredicate seqnoPredicate = new SeqnoWatchPredicate(seqno);
        return waitForEvent(seqnoPredicate, seqnoWatches, cancel);
    }

    /**
     * Sets a watch for a particular event ID to be extracted.
     * 
     * @param eventId Native event ID to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedEventId(
            String eventId, boolean cancel) throws InterruptedException
    {
        EventIdWatchPredicate eventPredicate = new EventIdWatchPredicate(
                eventId);
        return waitForEvent(eventPredicate, eventIdWatches, cancel);
    }

    /**
     * Sets a watch for a heartbeat event to be extracted.
     * 
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedHeartbeat(
            String name, boolean cancel) throws InterruptedException
    {
        HeartbeatWatchPredicate predicate = new HeartbeatWatchPredicate(name);
        // For heartbeats we always want the next one and don't care if
        // there was one before. This prevents confusion in the event that
        // the last event processed happened to be a heartbeat.
        if (cancel)
            return heartbeatWatches.watch(predicate, threadCount, cancelAction);
        else
            return heartbeatWatches.watch(predicate, threadCount);
    }

    /**
     * Sets a watch for a particular source timestamp to be extracted.
     * 
     * @param timestamp Timestame to watch for
     * @param cancel If true, terminate task when watch is successful
     * @return Returns a watch on the matching event
     * @throws InterruptedException
     */
    public synchronized Future<ReplDBMSHeader> watchForProcessedTimestamp(
            Timestamp timestamp, boolean cancel) throws InterruptedException
    {
        SourceTimestampWatchPredicate predicate = new SourceTimestampWatchPredicate(
                timestamp);
        return waitForEvent(predicate, timestampWatches, cancel);
    }

    /**
     * Private utility to set a watch of arbitrary type. This *must* be
     * synchronized to ensure we compute minimum events correctly.
     */
    private Future<ReplDBMSHeader> waitForEvent(
            WatchPredicate<ReplDBMSHeader> predicate,
            WatchManager<ReplDBMSHeader> manager, boolean cancel)
            throws InterruptedException
    {
        // Find the trailing event that has been processed across all
        // tasks.
        ReplDBMSHeader lastEvent = getMinLastEvent();
        Watch<ReplDBMSHeader> watch;
        if (lastEvent == null || !predicate.match(lastEvent))
        {
            // We have not reached the requested event, so we have to enqueue
            // a watch.
            if (cancel)
                watch = manager.watch(predicate, threadCount, cancelAction);
            else
                watch = manager.watch(predicate, threadCount);
            offerAll(watch);

            // If there is an upstream parallel store we need to ensure there
            // are synchronization events on all queues.
            if (upstreamStore != null)
                upstreamStore.insertWatchSyncEvent(watch.getPredicate());
        }
        else
        {
            // We have already reached it, so signal that we are cancelled, post
            // an interrupt flag, and return the current event.
            watch = new Watch<ReplDBMSHeader>(predicate, threadCount);
            offerAll(watch);
            if (cancel)
            {
                cancelAll();
                shouldInterruptTask = true;
            }
        }

        return watch;
    }

    // Offers the watch to each task in succession. This operation ensures
    // watches are correctly initialized in the event that some threads but
    // not others have satisfied the watch predicate.
    private void offerAll(Watch<ReplDBMSHeader> watch)
            throws InterruptedException
    {
        for (int i = 0; i < this.taskInfo.length; i++)
        {
            ReplDBMSHeader event = taskInfo[i].getLastEvent();
            if (event != null)
                watch.offer(event, i);
        }
    }

    /**
     * Returns false if the current event should be skipped.
     */
    public synchronized boolean skip(ReplDBMSEvent event)
    {
        // If we are skipping the first N transactions to be applied,
        // try again.
        if (this.applySkipCount > 0)
        {
            logger.info("Skipping event: seqno=" + event.getSeqno()
                    + " fragno=" + event.getFragno(), null);
            if (event.getLastFrag())
                applySkipCount--;
            return true;
        }
        else if (this.seqnosToBeSkipped != null)
        {
            // Purge skip numbers processing has already reached.
            long minSeqno = getMinLastSeqno();
            while (!this.seqnosToBeSkipped.isEmpty()
                    && this.seqnosToBeSkipped.first() < minSeqno)
                this.seqnosToBeSkipped.remove(this.seqnosToBeSkipped.first());

            if (!this.seqnosToBeSkipped.isEmpty())
            {
                // If we are in the skip list, then skip!
                if (seqnosToBeSkipped.contains(event.getSeqno()))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Skipping event with seqno "
                                + event.getSeqno());
                    // Skip event and remove seqno after last fragment.
                    if (event.getLastFrag())
                        this.seqnosToBeSkipped.remove(event.getSeqno());
                    return true;
                }
                // else seqnosToBeSkipped.first() > event.getSeqno()
                // so let's process this event
            }
            else
            {
                // the list is now empty... just free the list
                this.seqnosToBeSkipped = null;
                if (logger.isDebugEnabled())
                    logger.debug("No more events to be skipped");
            }
        }

        // No match, so we will process the event.
        return false;
    }
}