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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ReplDBMSFilteredEvent extends ReplDBMSEvent
{
    private long seqnoEnd = -1;
    private short fragnoEnd = -1;

    public ReplDBMSFilteredEvent(String lastFilteredId, Long firstFilteredSeqno,
            Long lastFilteredSeqno, Short lastFragno)
    {
        super(firstFilteredSeqno, new DBMSEvent(lastFilteredId));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragnoEnd = lastFragno;
    }
    
    public ReplDBMSFilteredEvent(Long firstFilteredSeqno,
            Short firstFilteredFragno, Long lastFilteredSeqno,
            Short lastFilteredFragno, boolean lastFrag, String eventId,
            String sourceId, Timestamp timestamp)
    {
        super(firstFilteredSeqno, new DBMSEvent(eventId, null, timestamp));
        this.seqnoEnd = lastFilteredSeqno;
        this.fragno = firstFilteredFragno;
        this.fragnoEnd = lastFilteredFragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
    }

    public ReplDBMSFilteredEvent(ReplDBMSHeader firstFilteredEvent,
            ReplDBMSHeader lastFilteredEvent)
    {
        super(firstFilteredEvent.getSeqno(), new DBMSEvent(lastFilteredEvent.getEventId()));
        this.seqnoEnd = lastFilteredEvent.getSeqno();
        this.fragno = firstFilteredEvent.getFragno();
        this.fragnoEnd = lastFilteredEvent.getFragno();
        this.lastFrag = lastFilteredEvent.getLastFrag();
        this.sourceId = firstFilteredEvent.getSourceId();
        this.epochNumber = firstFilteredEvent.getEpochNumber();
        this.shardId = firstFilteredEvent.getShardId();
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Returns the seqnoEnd value.
     * 
     * @return Returns the seqnoEnd.
     */
    public long getSeqnoEnd()
    {
        return seqnoEnd;
    }

    public void updateCommitSeqno()
    {
        this.seqno = this.seqnoEnd;
    }

    /**
     * Returns the fragnoEnd value.
     * 
     * @return Returns the fragnoEnd.
     */
    public short getFragnoEnd()
    {
        return fragnoEnd;
    }

}
