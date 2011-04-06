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

import java.io.DataOutputStream;
import java.io.IOException;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class encapsulates operations to write a log rotate event.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class LogEventRotateWriter
{
    // Inputs
    private long    index;
    private boolean checkCRC;

    /**
     * Instantiate the writer.
     */
    public LogEventRotateWriter(long index, boolean checkCRC)
            throws THLException
    {
        this.index = index;
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
            dos.writeByte(LogRecord.EVENT_ROTATE);
            dos.writeLong(index);
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