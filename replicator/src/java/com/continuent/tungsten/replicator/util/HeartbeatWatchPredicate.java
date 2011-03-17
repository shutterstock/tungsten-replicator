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

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;

/**
 * Implements a WatchPredicate that returns true when we see an event that is
 * marked as a heartbeat.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HeartbeatWatchPredicate implements WatchPredicate<ReplDBMSEvent>
{
    private final String name;
    private final boolean matchAny; 

    public HeartbeatWatchPredicate(String name)
    {
        this.name = name; 
        matchAny = "*".equals(name) || name == null;
    }

    /**
     * Return true if the sequence number is greater than or equal to what we
     * are seeking. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.util.WatchPredicate#match(java.lang.Object)
     */
    public boolean match(ReplDBMSEvent event)
    {
        if (event == null)
            return false;
        else
        {
            String heartbeatName = event.getDBMSEvent().getMetadataOptionValue(
                    ReplOptionParams.HEARTBEAT);
            if (heartbeatName != null)
            {
                if (matchAny)
                    return true;
                else 
                    return name.equals(heartbeatName);
            }
            else
                return false;
        }
    }
}