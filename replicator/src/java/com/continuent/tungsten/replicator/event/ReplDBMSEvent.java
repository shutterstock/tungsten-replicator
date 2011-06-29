/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.event;

import java.sql.Timestamp;
import java.util.ArrayList;

import com.continuent.tungsten.replicator.dbms.DBMSData;

/**
 * Storage class for replication events implementing full event management
 * metadata such as timestamp, source ID, epoch number, and event fragment
 * protocol.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ReplDBMSEvent extends ReplEvent implements ReplDBMSHeader
{
    static final long serialVersionUID = 1300;

    long              seqno;
    short             fragno;
    boolean           lastFrag;
    Timestamp         extractedTstamp;
    String            sourceId;
    String            shardId;
    long              epochNumber;
    DBMSEvent         event;

    /**
     * Construct a new replication event.
     * 
     * @param seqno Log sequence number
     * @param fragno Fragment number
     * @param lastFrag True if this is the last fragment
     * @param sourceId Originating source of data
     * @param epochNumber Epoch number on data
     * @param extractedTstamp Time of extraction
     * @param event Raw event data, which must always be supplied.
     */
    public ReplDBMSEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId, long epochNumber, Timestamp extractedTstamp,
            DBMSEvent event)
    {
        // All fields must exist to protect against failures. We therefore
        // validate object instances.
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.epochNumber = epochNumber;
        if (sourceId == null)
            this.sourceId = "NONE";
        else
            this.sourceId = sourceId;
        if (extractedTstamp == null)
            this.extractedTstamp = new Timestamp(System.currentTimeMillis());
        else
            this.extractedTstamp = extractedTstamp;
        if (event == null)
            this.event = new DBMSEvent();
        else
            this.event = event;
    }

    /**
     * Short constructor.
     */
    public ReplDBMSEvent(long seqno, DBMSEvent event)
    {
        this(seqno, (short) 0, true, "NONE", 0, new Timestamp(
                System.currentTimeMillis()), event);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getSeqno()
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getFragno()
     */
    public short getFragno()
    {
        return fragno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getLastFrag()
     */
    public boolean getLastFrag()
    {
        return lastFrag;
    }

    /**
     * Gets the event data for this replicated event.
     */
    public ArrayList<DBMSData> getData()
    {
        if (event != null)
            return event.getData();
        else
            return new ArrayList<DBMSData>();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getSourceId()
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getEpochNumber()
     */
    public long getEpochNumber()
    {
        return epochNumber;
    }

    /**
     * Returns the extractedTstamp value.
     * 
     * @return Returns the extractedTstamp.
     */
    public Timestamp getExtractedTstamp()
    {
        return extractedTstamp;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getEventId()
     */
    public String getEventId()
    {
        return event.getEventId();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.event.ReplDBMSHeader#getShardId()
     */
    public String getShardId()
    {
        String shardId = getDBMSEvent().getMetadataOptionValue(
                ReplOptionParams.SHARD_ID);
        if (shardId == null)
            return ReplOptionParams.SHARD_ID_UNKNOWN;
        else
            return shardId;
    }

    /**
     * Sets the shard ID. This can be assigned after the event is created.
     */
    public void setShardId(String shardId)
    {
        this.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                shardId);
    }

    /**
     * Returns the raw DBMS event containing SQL data.
     */
    public DBMSEvent getDBMSEvent()
    {
        return event;
    }
}