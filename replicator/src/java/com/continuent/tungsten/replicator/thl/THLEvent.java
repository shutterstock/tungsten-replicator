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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.io.Serializable;
import java.sql.Timestamp;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;

/**
 * This class defines a THLEvent
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class THLEvent implements Serializable
{
    static final long         serialVersionUID   = -1;

    /* Event types */
    /** Event carrying DBMS event information */
    static public final short REPL_DBMS_EVENT    = 0;
    /** This event is created each time node gets into master state */
    static public final short START_MASTER_EVENT = 1;
    /**
     * This event is created each time master node exits master state gracefully
     */
    static public final short STOP_MASTER_EVENT  = 2;
    /** Heartbeat event */
    static public final short HEARTBEAT_EVENT    = 3;

    /* THL event status codes */
    /**
     * Event is inserted into THL storage but it is not processed yet in any
     * way.
     */
    static public final short PENDING            = 0;
    /** Event is in applying stage. */
    static public final short IN_PROCESS         = 1;
    /** Event has been applied successfully. */
    static public final short COMPLETED          = 2;
    /** Applying event has failed for some reason. */
    static public final short FAILED             = 3;
    /** Event should be skipped automatically on its turn. */
    static public final short SKIP               = 4;
    /**
     * Event has been skipped either due to applying failure or because its
     * state was earlier set to SKIP
     */
    static public final short SKIPPED            = 5;

    private final long        seqno;
    private final short       fragno;
    private final boolean     lastFrag;
    private final String      sourceId;
    private final short       type;
    private final long        epochNumber;
    private final Timestamp   sourceTstamp;
    private final Timestamp   localEnqueueTstamp;
    private final Timestamp   processedTstamp;
    private short             status;
    private String            comment;
    private final String      eventId;
    private final ReplEvent   event;

    /**
     * Creates a new <code>THLEvent</code> object with status set to COMPLETED.
     * 
     * @param eventId Event identifier
     * @param event ReplDBMSEvent
     */
    public THLEvent(String eventId, ReplDBMSEvent event)
    {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.type = REPL_DBMS_EVENT;
        this.epochNumber = event.getEpochNumber();
        this.localEnqueueTstamp = null;
        this.sourceTstamp = event.getDBMSEvent().getSourceTstamp();
        this.processedTstamp = null;
        this.status = COMPLETED;
        this.comment = null;
        this.eventId = eventId;
        this.event = event;
    }

    /**
     * Creates a new <code>THLEvent</code> object with initial status
     * 
     * @param event Event
     * @param initialStatus Initial status for the event
     */
    public THLEvent(ReplDBMSEvent event, short initialStatus)
    {
        this.seqno = event.getSeqno();
        this.fragno = event.getFragno();
        this.lastFrag = event.getLastFrag();
        this.sourceId = event.getSourceId();
        this.type = REPL_DBMS_EVENT;
        this.epochNumber = event.getEpochNumber();
        this.localEnqueueTstamp = null;
        this.sourceTstamp = event.getDBMSEvent().getSourceTstamp();
        this.processedTstamp = null;
        this.status = initialStatus;
        this.comment = null;
        this.eventId = event.getEventId();
        this.event = event;

    }

    /**
     * Creates a new <code>THLEvent</code> object
     * 
     * @param seqno Sequence number
     * @param fragno Fragment number
     * @param lastFrag Last fragment flag
     * @param sourceId Source identifier
     * @param type Event type
     * @param localEnqueueTstamp Local enqueue timestamp
     * @param sourceTstamp Source timestamp
     * @param processedTstamp Processed timestamp
     * @param status Status
     * @param comment Comment
     * @param eventId Event identifier
     * @param event Event
     */
    public THLEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId, short type, long epochNumber,
            Timestamp localEnqueueTstamp, Timestamp sourceTstamp,
            Timestamp processedTstamp, short status, String comment,
            String eventId, ReplEvent event)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.sourceId = sourceId;
        this.type = type;
        this.epochNumber = epochNumber;
        this.localEnqueueTstamp = localEnqueueTstamp;
        this.sourceTstamp = sourceTstamp;
        this.processedTstamp = processedTstamp;
        this.status = status;
        this.comment = comment;
        this.eventId = eventId;
        this.event = event;
    }

    /**
     * Get event sequence number.
     * 
     * @return Sequence number
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * Get event fragment number.
     * 
     * @return Fragment number
     */
    public short getFragno()
    {
        return fragno;
    }

    /**
     * Get last fragment flag.
     * 
     * @return Last fragment flag
     */
    public boolean getLastFrag()
    {
        return lastFrag;
    }

    /**
     * Get source identifier.
     * 
     * @return Source identifier
     */
    public String getSourceId()
    {
        return sourceId;
    }

    /**
     * Get event type.
     * 
     * @return Event type
     */
    public short getType()
    {
        return type;
    }

    /**
     * Get event epoch number.
     * 
     * @return Epoch number
     */
    public long getEpochNumber()
    {
        return epochNumber;
    }

    /**
     * Get local enqueue timestamp.
     * 
     * @return Local enqueue timestamp
     */
    public Timestamp getLocalEnqueueTstamp()
    {
        return localEnqueueTstamp;
    }

    /**
     * Get source timestamp.
     * 
     * @return Source timestamp
     */
    public Timestamp getSourceTstamp()
    {
        return sourceTstamp;
    }

    /**
     * Get processed timestamp.
     * 
     * @return Processed timestamp
     */
    public Timestamp getProcessedTstamp()
    {
        return processedTstamp;
    }

    /**
     * Set event status and comment.
     * 
     * @param status New status
     * @param comment New comment
     */
    public void setStatus(short status, String comment)
    {
        this.status = status;
        this.comment = comment;
    }

    /**
     * Get event status.
     * 
     * @return Status
     */
    public short getStatus()
    {
        return status;
    }

    /**
     * Get event comment.
     * 
     * @return Comment
     */
    public String getComment()
    {
        return comment;
    }

    /**
     * Get event identifier.
     * 
     * @return event's id
     */
    public String getEventId()
    {
        return eventId;
    }

    /**
     * Get associated replication.
     * 
     * @return associated ReplEvent
     */
    public ReplEvent getReplEvent()
    {
        return event;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        String ret = "seqno=" + seqno;
        ret += " fragno=" + fragno;
        ret += " lastFrag=" + lastFrag;
        ret += " sourceId=" + sourceId;
        ret += " type=" + type;
        ret += " localEnqueueTstamp=" + localEnqueueTstamp;
        ret += " sourceTstamp=" + sourceTstamp;
        ret += " processedTstamp=" + processedTstamp;
        ret += " status=" + status;
        ret += " comment=" + comment;
        ret += " eventId=" + eventId;
        ret += " event=" + event.toString();
        return ret;
    }

    /**
     * Return human readable interpretation of
     * the 'status'
     * 
     * @return status as a string(number)
     */
    static public String statusToString(int status)
    {
        String value = "UNKNOWN";
        switch (status)
        {
            case PENDING :
                value = "PENDING";
                break;
            case IN_PROCESS :
                value = "IN_PROCESS";
                break;
            case COMPLETED :
                value = "COMPLETED";
                break;
            case FAILED :
                value = "FAILED";
                break;
            case SKIP :
                value = "SKIP";
                break;
            case SKIPPED :
                value = "SKIPPED";
                break;
            default :
                value = "UNKNOWN";
        }

        return String.format("%s(%d)", value, status);
    }

}
