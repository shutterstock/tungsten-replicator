/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2009 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.util;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Implements a WatchPredicate to identify that a particular native event ID has
 * been reached. This returns true for any event ID equal to
 * <em>or higher than</em> the number we are seeking.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventIdWatchPredicate implements WatchPredicate<ReplDBMSHeader>
{
    private final String eventId;

    public EventIdWatchPredicate(String eventId)
    {
        this.eventId = eventId;
    }

    public boolean match(ReplDBMSHeader event)
    {
        if (event == null)
            return false;
        else if (event.getEventId() == null)
            return false;
        else if (event.getEventId().compareTo(eventId) < 0)
            return false;
        else
            return true;
    }

    /**
     * Returns the class name and the event id for which we waiting.
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return this.getClass().getName() + " eventId=" + eventId;
    }
}