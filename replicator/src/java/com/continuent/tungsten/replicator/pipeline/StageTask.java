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

import java.util.SortedSet;

import com.continuent.tungsten.commons.patterns.event.EventDispatcher;

/**
 * Denotes a Runnable task that implements stage processing.  Set methods are
 * called before the run() method. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface StageTask extends Runnable
{
    /**
     * Provides the stage instance that this runnable operates on.
     */
    public void setStage(Stage stage);

    /**
     * Provides an event dispatcher to log error.
     */
    public void setEventDispatcher(EventDispatcher eventDispatcher);

    /** 
     * Sets number of apply operations to skip at start-up. 
     */
    public void setApplySkipCount(long applySkipCount);

    /**
     * Sets the schedule, which controls task cancellation. 
     */
    public void setSchedule(Schedule schedule);

    public void setApplySkipSeqnos(SortedSet<Long> seqnosToBeSkipped);
}