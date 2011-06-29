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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl.log;

import java.io.DataInputStream;
import java.io.IOException;

import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.serializer.Serializer;

/**
 * This class encapsulates operations to read a log record header and serialized
 * THLEvent for an event. It automatically reads the header but does not
 * deserialize the event until asked to. You should call done() after use to
 * free resources.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventReplReader
{
    // Inputs
    private LogRecord         logRecord;
    private Serializer        serializer;
    private boolean           checkCRC;

    // Stream used to read the event.
    private DataInputStream dis;

    // Header fields
    private byte              recordType;
    private long              seqno;
    private short             fragno;
    private boolean           lastFrag;
    private long              epochNumber;
    private String            sourceId;
    private String            eventId;
    private String            shardId;
    private Long              sourceTStamp;

    /**
     * Instantiate the reader and load header information.
     */
    public LogEventReplReader(LogRecord logRecord, Serializer serializer,
            boolean checkCRC) throws THLException
    {
        this.logRecord = logRecord;
        this.serializer = serializer;
        this.checkCRC = checkCRC;
        try
        {
            load();
        }
        catch (IOException e)
        {
            throw new THLException(
                    "I/O error while loading log record header: offset="
                            + logRecord.getOffset(), e);
        }
    }

    // Load header fields.
    private void load() throws THLException, IOException
    {
        // Check CRC if requested.
        if (checkCRC)
        {
            if (!logRecord.checkCrc())
            {
                throw new THLException("Log record CRC failure: offset="
                        + logRecord.getOffset() + " crc type="
                        + logRecord.getCrcType() + " stored crc="
                        + logRecord.getCrc() + " computed crc="
                        + logRecord.getCrc());
            }
        }

        // Read the header fields.
        dis = new DataInputStream(logRecord.read());
        recordType = dis.readByte();
        if (recordType != LogRecord.EVENT_REPL)
            throw new THLException("Invalid log record type reader: offset="
                    + logRecord.getOffset() + " type=" + recordType);
        seqno = dis.readLong();
        fragno = dis.readShort();
        lastFrag = (dis.readByte() == 1);
        epochNumber = dis.readLong();
        sourceId = dis.readUTF();
        eventId = dis.readUTF();
        shardId = dis.readUTF();
        sourceTStamp = dis.readLong();
    }

    public LogRecord getLogRecord()
    {
        return logRecord;
    }

    public byte getRecordType()
    {
        return recordType;
    }

    public long getSeqno()
    {
        return seqno;
    }

    public short getFragno()
    {
        return fragno;
    }

    public boolean isLastFrag()
    {
        return lastFrag;
    }

    public long getEpochNumber()
    {
        return epochNumber;
    }

    public String getSourceId()
    {
        return sourceId;
    }

    public String getEventId()
    {
        return eventId;
    }

    public String getShardId()
    {
        return shardId;
    }

    public Long getSourceTStamp()
    {
        return sourceTStamp;
    }

    /** Deserialize and return the event. */
    public THLEvent deserializeEvent() throws THLException
    {
        try
        {
            THLEvent thlEvent = serializer.deserializeEvent(dis);
            return thlEvent;
        }
        catch (IOException e)
        {
            throw new THLException("Unable to deserialize event", e);
        }
    }

    /** Release the log record. */
    public void done()
    {
        logRecord.done();
        logRecord = null;
    }
}