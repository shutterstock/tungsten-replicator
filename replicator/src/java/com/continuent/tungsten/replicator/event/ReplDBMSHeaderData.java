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

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;

/**
 * An implementation of replicator header information used to track position.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplDBMSHeaderData implements ReplDBMSHeader
{
    private final long      seqno;
    private final short     fragno;
    private final boolean   lastFrag;
    private final String    sourceId;
    private final long      epochNumber;
    private final String    eventId;
    private final String    shardId;
    private final Timestamp extractedTstamp;

    /**
     * Create header instance from component parts.
     */
    public ReplDBMSHeaderData(long seqno, short fragno, boolean lastFrag,
            String sourceId, long epochNumber, String eventId, String shardId,
            Timestamp extractedTstamp)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.epochNumber = epochNumber;
        this.eventId = eventId;
        this.shardId = shardId;
        this.extractedTstamp = extractedTstamp;
    }

    public ReplDBMSHeaderData(ReplDBMSHeader event)
    {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.epochNumber = event.getEpochNumber();
        this.eventId = event.getEventId();
        this.shardId = event.getShardId();
        this.extractedTstamp = event.getExtractedTstamp();
    }

    public long getSeqno()
    {
        return seqno;
    }

    public String getEventId()
    {
        return eventId;
    }

    public long getEpochNumber()
    {
        return epochNumber;
    }

    public short getFragno()
    {
        return fragno;
    }

    public boolean getLastFrag()
    {
        return lastFrag;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getShardId()
    {
        return shardId;
    }

    public Timestamp getExtractedTstamp()
    {
        return extractedTstamp;
    }
}