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
 * Initial developer(s):  Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.pipeline;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;

/**
 * Denotes a schedule, which monitors and directs task execution.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Schedule
{
    /**
     * Task must call this method after extracting event but before processing
     * to decide disposition.
     * 
     * @return A disposition for the event.
     * @throws InterruptedException Thrown if thread is interrupted
     */
    public int advise(ReplEvent replEvent) throws InterruptedException;

    /**
     * Task must call this method before exit to tell the schedule that it has
     * completed.
     */
    public void taskEnd();

    /**
     * Set the last processed event, which triggers checks for watches. If a
     * fulfilled watch directs the task to terminate, the isCancelled call will
     * return true.
     * 
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public void setLastProcessedEvent(ReplDBMSEvent event)
            throws InterruptedException;

    /**
     * Marks the last processed exception as committed. This information is used
     * by upstream stages that implement synchronous pipeline processing, e.g.,
     * not dropping logs before they are safely committed downstream.
     * 
     * @throws InterruptedException Thrown if thread is interrupted.
     */
    public void commit() throws InterruptedException;

    /**
     * Returns true if the task is canceled. Tasks must check this each
     * iteration to decide whether to continue.
     */
    public boolean isCancelled();

    // Processing dispositions for events.

    /** Proceed with event processing. */
    public static int PROCEED              = 1;

    /** Commit current transaction and terminate task processing loop. */
    public static int QUIT                 = 2;

    /** Continue with the next event. */
    public static int CONTINUE_NEXT        = 3;

    /** Continue with next event but commit current position. */
    public static int CONTINUE_NEXT_COMMIT = 4;
}