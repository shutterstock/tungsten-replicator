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

import java.io.DataOutputStream;
import java.io.IOException;

import com.continuent.tungsten.replicator.thl.serializer.Serializer;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class encapsulates operations to write a log record header and
 * serialized THLEvent. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventReplWriter
{
    // Inputs
    private THLEvent           event;
    private Serializer         serializer;
    private boolean            checkCRC;

    /**
     * Instantiate the writer. 
     */
    public LogEventReplWriter(THLEvent event, Serializer serializer,
            boolean checkCRC) throws THLException
    {
        this.event = event;
        this.serializer = serializer;
        this.checkCRC = checkCRC;
    }

    /**
     * Write and return the log record. 
     */
    public LogRecord write() throws THLException
    {
        LogRecord logRecord = new LogRecord(-1, checkCRC);
        try
        {
            DataOutputStream dos = new DataOutputStream(logRecord.write());
            dos.writeByte(LogRecord.EVENT_REPL);
            dos.writeLong(event.getSeqno());
            dos.writeShort(event.getFragno());
            dos.writeByte((event.getLastFrag() ? 1 : 0));
            dos.writeLong(event.getEpochNumber());
            dos.writeUTF(event.getSourceId());
            dos.writeUTF(event.getEventId());
            dos.writeUTF(event.getShardId());
            dos.writeLong(event.getSourceTstamp().getTime());
            
            serializer.serializeEvent(event, dos);
            dos.flush();
            logRecord.done();

            if (checkCRC)
                logRecord.storeCrc(LogRecord.CRC_TYPE_32);
        }
        catch (IOException e)
        {
            throw new THLException("Error writing log record data: "
                    + e.getMessage(), e);
        }
        finally
        {
            logRecord.done();
        }
        
        return logRecord;
    }
}