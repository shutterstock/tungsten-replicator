/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.io.Serializable;

/**
 * This class is the superclass from which all replication events inherit. It
 * defines minimal shared behavior. This is currently restricted to providing a
 * common serialization interface and estimated size to help with memory
 * management. Estimated size is a hint and does not have to be exact. It is
 * designed to help us tell whether the object in question needs a lot of heap
 * memory.
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public abstract class ReplEvent implements Serializable
{
    private static final long serialVersionUID = 1300;
    private transient int     estimatedSize;

    public ReplEvent()
    {
    }

    /** 
     * Returns the sequence number of this event. 
     */
    public abstract long getSeqno();
    
    /**
     * Returns the estimated serialized size of this event, if known.
     */
    public int getEstimatedSize()
    {
        return estimatedSize;
    }

    /**
     * Sets the estimated serialized size of this event.
     */
    public void setEstimatedSize(int estimatedSize)
    {
        this.estimatedSize = estimatedSize;
    }
}
