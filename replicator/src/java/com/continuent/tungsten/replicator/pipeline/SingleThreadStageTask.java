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

import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ErrorNotification;
import com.continuent.tungsten.replicator.EventDispatcher;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierException;
import com.continuent.tungsten.replicator.conf.FailurePolicy;
import com.continuent.tungsten.replicator.event.ReplControlEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements thread logic for single-threaded, i.e., non-parallel stage
 * execution.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class SingleThreadStageTask implements Runnable
{
    private static Logger   logger          = Logger.getLogger(SingleThreadStageTask.class);
    private Stage           stage;
    private int             taskId;
    private Extractor       extractor;
    private List<Filter>    filters;
    private Applier         applier;
    private String          loggingPrefix   = "[]";
    private boolean         usingBlockCommit;
    private int             blockCommitRowsCount;
    private EventDispatcher eventDispatcher;
    private Schedule        schedule;
    private String          name;

    private long            blockEventCount = 0;
    private TaskProgress    taskProgress;

    public SingleThreadStageTask(Stage stage, int taskId)
    {
        this.taskId = taskId;
        this.name = stage.getName() + "-" + taskId;
        this.loggingPrefix = "[" + name + "] ";
        this.stage = stage;
        this.blockCommitRowsCount = stage.getBlockCommitRowCount();
        this.usingBlockCommit = (blockCommitRowsCount > 1);
        this.taskProgress = stage.getProgressTracker().getTaskProgress(taskId);
    }

    /** Returns the id of this task. */
    public int getTaskId()
    {
        return taskId;
    }

    /**
     * Sets the event dispatcher
     *
     * @see com.continuent.tungsten.replicator.pipeline.StageTask#setEventDispatcher(com.continuent.tungsten.replicator.EventDispatcher)
     */
    public void setEventDispatcher(EventDispatcher eventDispatcher)
    {
        this.eventDispatcher = eventDispatcher;
    }

    /** Sets the schedule instance used to control loop continuation. */
    public void setSchedule(Schedule schedule)
    {
        this.schedule = schedule;
    }

    public void setExtractor(Extractor extractor)
    {
        this.extractor = extractor;
    }

    public void setFilters(List<Filter> filters)
    {
        this.filters = filters;
    }

    public void setApplier(Applier applier)
    {
        this.applier = applier;
    }

    public Extractor getExtractor()
    {
        return extractor;
    }

    public List<Filter> getFilters()
    {
        return filters;
    }

    public Applier getApplier()
    {
        return applier;
    }

    public String getName()
    {
        return name;
    }

    /**
     * Perform thread processing logic.
     */
    public void run()
    {
        logInfo("Starting stage task thread", null);
        taskProgress.begin();

        runTask();

        logInfo("Terminating processing for stage", null);
        ReplDBMSEvent lastEvent = stage.getProgressTracker()
                .getLastProcessedEvent(taskId);
        if (lastEvent != null)
        {
            String msg = "Last successfully processed event prior to termination: seqno="
                    + lastEvent.getSeqno()
                    + " eventid="
                    + lastEvent.getEventId();
            logInfo(msg, null);
        }
        logInfo("Stage event count: " + taskProgress.getEventCount(), null);
        schedule.taskEnd();
    }

    /**
     * Perform single-threaded stage processing.
     *
     * @throws ReplicatorException
     */
    public void runTask()
    {
        PluginContext context = stage.getPluginContext();

        ReplDBMSEvent currentEvent = null;
        ReplDBMSEvent firstFilteredEvent = null;
        ReplDBMSEvent lastFilteredEvent = null;

        ReplEvent genericEvent = null;
        ReplDBMSEvent event = null;
        try
        {
            // If we are supposed to auto-synchronize, do it now.
            if (stage.isAutoSync())
            {
                // Indicate that we are ready to go.
                eventDispatcher.handleEvent(new InSequenceNotification());
            }
            boolean syncTHLWithExtractor = stage.getPipeline()
                    .syncTHLWithExtractor();

            while (!Thread.currentThread().isInterrupted())
            {
                // If we have a pending currentEvent from the last iteration,
                // we should log it now, then test to see whether the task has
                // been cancelled.
                if (currentEvent != null && firstFilteredEvent == null)
                {
                    schedule.setLastProcessedEvent(currentEvent);
                    currentEvent = null;
                }

                // Check for cancellation and exit loop if it has occurred.
                if (schedule.isCancelled())
                {
                    logInfo("Task has been cancelled", null);
                    break;
                }

                // Fetch the next event.
                event = null;
                try
                {
                    taskProgress.beginInterval();
                    genericEvent = extractor.extract();
                    taskProgress.endExtractInterval();
                }
                catch (ExtractorException e)
                {
                    String message = "Event extraction failed: "
                            + e.getMessage();
                    if (context.getExtractorFailurePolicy() == FailurePolicy.STOP)
                    {
                        eventDispatcher.handleEvent(new ErrorNotification(
                                message, e));
                        break;
                    }
                    else
                    {
                        logError(message, e);
                        continue;
                    }
                }

                // Retry if no event returned; debug logging goes here.
                if (genericEvent == null)
                {
                    if (logger.isDebugEnabled())
                        logger.debug(loggingPrefix
                                + "No event extracted, retrying...");
                    currentEvent = null;
                    continue;
                }

                // Submit the event to the schedule to see what we should do
                // with it.
                int disposition = schedule.advise(genericEvent);
                if (disposition == Schedule.PROCEED)
                {
                    // Go ahead and apply this event.
                }
                else if (disposition == Schedule.CONTINUE_NEXT)
                {
                    updatePosition(genericEvent, false);
                    currentEvent = null;
                    continue;
                }
                else if (disposition == Schedule.CONTINUE_NEXT_COMMIT)
                {
                    updatePosition(genericEvent, true);
                    currentEvent = null;
                    continue;
                }
                else if (disposition == Schedule.QUIT)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Quitting task processing loop");
                    updatePosition(genericEvent, false);
                    break;
                }
                else
                {
                    // This is a serious bug.
                    throw new ReplicatorException(
                            "Unexpected schedule disposition on event: disposition="
                                    + disposition + " event="
                                    + genericEvent.toString());
                }

                // Convert to a proper log event and proceed.
                event = (ReplDBMSEvent) genericEvent;
                if (logger.isDebugEnabled())
                {
                    logger.debug(loggingPrefix + "Extracted event: seqno="
                            + event.getSeqno() + " fragno=" + event.getFragno());
                }
                currentEvent = event;

                // Run filters.
                taskProgress.beginInterval();
                for (Filter f : filters)
                {
                    if ((event = f.filter(event)) == null)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug(loggingPrefix
                                    + "Event discarded by filter: name="
                                    + f.getClass().toString());
                        }
                        break;
                    }
                }
                taskProgress.endFilterInterval();

                // Event was filtered... Get next event.
                if (event == null)
                {
                    if (firstFilteredEvent == null)
                    {
                        firstFilteredEvent = currentEvent;
                        lastFilteredEvent = currentEvent;
                    }
                    else
                        lastFilteredEvent = currentEvent;
                    continue;
                }
                else
                {
                    // This event is not filtered. Check if there are pending
                    // filtered events that should be stored.
                    if (firstFilteredEvent != null)
                    {
                        try
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Applying filtered event");
                            }
                            taskProgress.beginInterval();
                            applier.apply(new ReplDBMSFilteredEvent(
                                    firstFilteredEvent, lastFilteredEvent),
                                    true, syncTHLWithExtractor);
                            taskProgress.endApplyInterval();
                            firstFilteredEvent = null;
                            lastFilteredEvent = null;
                        }
                        catch (ApplierException e)
                        {
                            String message = "Event application failed: seqno="
                                    + event.getSeqno() + " fragno="
                                    + event.getFragno() + " message="
                                    + e.getMessage();
                            logError(message, e);
                            if (context.getApplierFailurePolicy() == FailurePolicy.STOP)
                            {
                                eventDispatcher
                                        .handleEvent(new ErrorNotification(
                                                message, event.getSeqno(),
                                                event.getEventId(), e));
                                break;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                }

                // Commit when :
                // 1. block commit is not used AND this is the last
                // fragment of the transaction
                // 2. (When maximum number of events is reached
                // OR when queue is empty)
                // AND this is the last fragment of the transaction
                boolean doCommit = false;
                if (usingBlockCommit)
                {
                    blockEventCount++;
                    if (event.getLastFrag()
                            && ((blockEventCount >= blockCommitRowsCount) || !extractor
                                    .hasMoreEvents()))
                    {
                        doCommit = true;
                        blockEventCount = 0;
                    }
                }
                else
                {
                    doCommit = event.getLastFrag();
                }

                // Apply the event with optional commit.
                try
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug(loggingPrefix + "Applying event: seqno="
                                + event.getSeqno() + " fragno="
                                + event.getFragno() + " doCommit=" + doCommit);
                    }
                    taskProgress.beginInterval();
                    applier.apply(event, doCommit, syncTHLWithExtractor);
                    taskProgress.endApplyInterval();
                }
                catch (ApplierException e)
                {
                    String message = "Event application failed: seqno="
                            + event.getSeqno() + " fragno=" + event.getFragno()
                            + " message=" + e.getMessage();
                    logError(message, e);
                    if (context.getApplierFailurePolicy() == FailurePolicy.STOP)
                    {
                        eventDispatcher.handleEvent(new ErrorNotification(
                                message, event.getSeqno(), event.getEventId(),
                                e));
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }
            }

            // At the end of the loop, issue commit to ensure partial block
            // becomes persistent.
            applier.commit();
        }
        catch (InterruptedException e)
        {
            if (!schedule.isCancelled())
                logger.warn(loggingPrefix
                        + "Received unexpected interrupt in stage task: "
                        + stage.getName());
            // Roll back to release locks and clear partial work.
            try
            {
                applier.rollback();
            }
            catch (InterruptedException e1)
            {
                logWarn("Task cancelled while trying to rollback following cancellation",
                        null);
            }
        }
        catch (Throwable e)
        {
            // An unexpected error occurred.
            String msg = "Stage task failed: " + stage.getName();
            if (event == null)
            {
                dispatchErrorEvent(new ErrorNotification(msg, e));
            }
            else
            {
                dispatchErrorEvent(new ErrorNotification(msg, event.getSeqno(),
                        event.getEventId(), e));
            }
            logger.info("Unexpected error: " + msg, e);
        }
    }

    // Utility routine to update position. This routine knows about control
    // events and block commit.
    private void updatePosition(ReplEvent replEvent, boolean doCommit)
            throws ReplicatorException, InterruptedException
    {
        // Find an event we can use to update our position.
        ReplDBMSHeader header = null;
        if (replEvent instanceof ReplControlEvent)
        {
            ReplControlEvent controlEvent = (ReplControlEvent) replEvent;
            header = controlEvent.getEvent();
        }
        else if (replEvent instanceof ReplDBMSEvent)
        {
            header = (ReplDBMSEvent) replEvent;
        }

        // Bail if the event we found is null.
        if (header == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to update position due to null event value");
            return;
        }

        // Decide whether to commit. This recapitulates logic in the main loop.
        if (usingBlockCommit)
        {
            blockEventCount++;
            if ((blockEventCount >= blockCommitRowsCount)
                    || !extractor.hasMoreEvents())
            {
                // Commit if we are at the end of the block.
                doCommit = true;
                blockEventCount = 0;
            }
            else
            {
                // Don't commit unless client really wants it.
                doCommit |= false;
            }
        }
        else
            doCommit = true;

        // Finally, update!
        if (logger.isDebugEnabled())
        {
            logger.debug(loggingPrefix + "Updating position: seqno="
                    + header.getSeqno() + " doCommit=" + doCommit);
        }
        taskProgress.beginInterval();
        applier.updatePosition(header, doCommit, false);
        taskProgress.endApplyInterval();
    }

    // Utility routine to log error event with exception handling.
    private void dispatchErrorEvent(ErrorNotification en)
    {
        try
        {
            eventDispatcher.handleEvent(en);
        }
        catch (InterruptedException e)
        {
            logWarn("Task cancelled while posting error notification", null);
        }
    }

    // Utility routines to print log messages with stage names.
    private void logInfo(String message, Throwable e)
    {
        String prefixedMessage = loggingPrefix + message;
        if (e == null)
            logger.info(prefixedMessage);
        else
            logger.info(prefixedMessage, e);
    }

    private void logWarn(String message, Throwable e)
    {
        String prefixedMessage = loggingPrefix + message;
        if (e == null)
            logger.warn(prefixedMessage);
        else
            logger.warn(prefixedMessage, e);
    }

    private void logError(String message, Throwable e)
    {
        String prefixedMessage = loggingPrefix + message;
        if (e == null)
            logger.error(prefixedMessage);
        else
            logger.error(prefixedMessage, e);
    }
}