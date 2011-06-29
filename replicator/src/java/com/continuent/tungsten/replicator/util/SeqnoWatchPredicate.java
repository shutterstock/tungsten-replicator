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
 * Implements a WatchPredicate to identify that a particular sequence number has
 * been reached. This returns true for any sequence number equal to
 * <em>or higher than</em> the number we are seeking.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SeqnoWatchPredicate implements WatchPredicate<ReplDBMSHeader>
{
    private final long seqno;

    public SeqnoWatchPredicate(long seqno)
    {
        this.seqno = seqno;
    }

    /**
     * Return true if the sequence number is greater than or equal to what we
     * are seeking. Note: If the event is the same, we must ensure it is the
     * last fragment.
     * 
     * @see com.continuent.tungsten.replicator.util.WatchPredicate#match(java.lang.Object)
     */
    public boolean match(ReplDBMSHeader event)
    {
        if (event == null)
            return false;
        else if (event.getSeqno() > seqno)
            return true;
        else if (event.getSeqno() == seqno && event.getLastFrag())
            return true;
        else
            return false;
    }

    public long getSeqno()
    {
        return seqno;
    }
}