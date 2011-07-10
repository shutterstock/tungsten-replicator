/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
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

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Tracks statistics for an individual task, which is identified by a task ID.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TaskProgress
{
    private final String   stageName;
    private final int      taskId;
    private ReplDBMSHeader lastEvent           = null;
    private boolean        cancelled           = false;
    private long           eventCount          = 0;
    private long           applyLatencyMillis  = 0;
    private long           startMillis;
    private long           totalExtractMillis  = 0;
    private long           totalFilterMillis   = 0;
    private long           totalApplyMillis    = 0;
    private TaskState      state               = TaskState.other;

    // Used to mark the beginning of a timing interval.
    private long           intervalStartMillis = 0;

    // Used to mark the end of the last known interval so we can
    // accurately compute elapsed time.
    private long           endMillis           = 0;

    /**
     * Defines a new task progress tracker for the given task ID.
     * 
     * @param stageName Name of stage to which task belongs
     * @param taskId Task ID number
     */
    TaskProgress(String stageName, int taskId)
    {
        this.stageName = stageName;
        this.taskId = taskId;
    }

    /**
     * Create a clone of a current instance.
     */
    public TaskProgress(TaskProgress other)
    {
        this.stageName = other.getStageName();
        this.taskId = other.getTaskId();
        this.applyLatencyMillis = other.getApplyLatencyMillis();
        this.cancelled = other.isCancelled();
        this.eventCount = other.getEventCount();
        this.lastEvent = other.getLastEvent();
        this.startMillis = other.getStartMillis();
        this.endMillis = other.getEndMillis();
        this.totalApplyMillis = other.getTotalApplyMillis();
        this.totalExtractMillis = other.getTotalExtractMillis();
        this.totalFilterMillis = other.getTotalFilterMillis();
        this.state = other.getState();
    }

    /**
     * Start the task progress timer. Should be called when a task thread begins
     * processing.
     */
    public void begin()
    {
        startMillis = System.currentTimeMillis();
        endMillis = startMillis;
    }

    public String getStageName()
    {
        return this.stageName;
    }

    public int getTaskId()
    {
        return this.taskId;
    }

    public ReplDBMSHeader getLastEvent()
    {
        return lastEvent;
    }

    public void setLastEvent(ReplDBMSHeader lastEvent)
    {
        this.lastEvent = lastEvent;
    }

    public boolean isCancelled()
    {
        return cancelled;
    }

    public void setCancelled(boolean cancelled)
    {
        this.cancelled = cancelled;
    }

    public long getEventCount()
    {
        return eventCount;
    }

    public void setEventCount(long eventCount)
    {
        this.eventCount = eventCount;
    }

    public void incrementEventCount()
    {
        this.eventCount++;
    }

    /** Return apply latency in milliseconds. Sub-zero values are rounded to 0. */
    public long getApplyLatencyMillis()
    {
        // Latency may be sub-zero due to clock differences.
        if (applyLatencyMillis < 0)
            return 0;
        else
            return applyLatencyMillis;
    }

    /** Return apply latency in seconds. */
    public double getApplyLatencySeconds()
    {
        long applyLatencyMillis = getApplyLatencyMillis();
        return applyLatencyMillis / 1000.0;
    }

    public void setApplyLatencyMillis(long applyLatencyMillis)
    {
        this.applyLatencyMillis = applyLatencyMillis;
    }

    /** Returns the start time of the task. */
    public long getStartMillis()
    {
        return startMillis;
    }

    /** Returns the current end time for measuring intervals. */
    public long getEndMillis()
    {
        return endMillis;
    }

    /** Returns cumulative extract time in milliseconds. */
    public long getTotalExtractMillis()
    {
        return totalExtractMillis;
    }

    /** Return extract time in seconds. */
    public double getTotalExtractSeconds()
    {
        return getTotalExtractMillis() / 1000.0;
    }

    /** Start an extract interval. */
    public void beginExtractInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.extract;
    }

    /** Add time for an extract operation interval. */
    public void endExtractInterval()
    {
        endMillis = System.currentTimeMillis();
        totalExtractMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns cumulative filter time in milliseconds */
    public long getTotalFilterMillis()
    {
        return totalFilterMillis;
    }

    /** Return filter time in seconds. */
    public double getTotalFilterSeconds()
    {
        return getTotalFilterMillis() / 1000.0;
    }

    /** Start a filter interval. */
    public void beginFilterInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.filter;
    }

    /** Add time for a filter operation interval. */
    public void endFilterInterval()
    {
        endMillis = System.currentTimeMillis();
        totalFilterMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns cumulative extract time in milliseconds. */
    public long getTotalApplyMillis()
    {
        return totalApplyMillis;
    }

    /** Return apply time in seconds. */
    public double getTotalApplySeconds()
    {
        return getTotalApplyMillis() / 1000.0;
    }

    /** Start an apply interval. */
    public void beginApplyInterval()
    {
        intervalStartMillis = System.currentTimeMillis();
        endMillis = intervalStartMillis;
        state = TaskState.apply;
    }

    /** Add time for an apply operation interval. */
    public void endApplyInterval()
    {
        endMillis = System.currentTimeMillis();
        totalApplyMillis += (endMillis - intervalStartMillis);
        state = TaskState.other;
    }

    /** Returns remaining wall-clock time outside of extract/filter/apply. */
    public long getTotalOtherMillis()
    {
        long remaining = endMillis - startMillis - totalExtractMillis
                - totalFilterMillis - totalApplyMillis;
        return remaining;
    }

    /** Return other time in seconds. */
    public double getTotalOtherSeconds()
    {
        return getTotalOtherMillis() / 1000.0;
    }

    /** Returns the current task state. */
    public TaskState getState()
    {
        return state;
    }

    /**
     * Returns a shallow copy of this instance.
     */
    public TaskProgress clone()
    {
        TaskProgress clone = new TaskProgress(this);
        return clone;
    }
}
