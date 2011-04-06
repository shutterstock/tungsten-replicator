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

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.DataInputStream;
import java.io.IOException;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class encapsulates operations to read a log rotate event.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventRotateReader
{
    // Inputs
    private LogRecord         logRecord;
    private boolean           checkCRC;

    // Stream used to read the event.
    private DataInputStream dis;

    // Fields
    private byte              recordType;
    private long              index;

    /**
     * Instantiate the reader and load header information.
     */
    public LogEventRotateReader(LogRecord logRecord, boolean checkCRC)
            throws THLException, IOException
    {
        this.logRecord = logRecord;
        this.checkCRC = checkCRC;
        load();
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
        index = dis.readLong();

        if (recordType != LogRecord.EVENT_ROTATE)
            throw new IOException("Invalid log record type reader: offset="
                    + logRecord.getOffset() + " type=" + recordType);
    }

    public byte getRecordType()
    {
        return recordType;
    }

    public long getIndex()
    {
        return index;
    }

    /** Release the log record. */
    public void done()
    {
        logRecord.done();
        logRecord = null;
    }
}