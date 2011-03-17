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

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Denotes a class that provides scheduling information for stage tasks.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface TaskScheduler
{
    /**
     * Controls whether stage task should continue processing events. This call
     * may do of the following:
     * <ul>
     * <li>Return true in which case stage should proceed to handle the next
     * event.
     * <li>Return false in which case the stage should exit.
     * <li>Pause (hang), then return one of the preceeding statuses
     * </ul>
     * The final case is how we implement a pause operation on stage tasks.
     * 
     * @param lastEvent Last event processed by this task
     */
    public boolean proceed(ReplDBMSHeader lastEvent);
}