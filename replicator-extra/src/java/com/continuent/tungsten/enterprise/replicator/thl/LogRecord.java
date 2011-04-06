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
 *
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Encapsulates a log record from the Tungsten disk log.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogRecord
{
    /**
     * Number of bytes in length field plus CRC. The record length is this
     * number plus the number of bytes of data (currently 4 + 1 + 8).
     */
    public static final int       NON_DATA_BYTES = 13;

    /** Denotes record header information. */
    public static final byte      EVENT_REPL     = 0x01;

    /** Denotes a replication event */
    public static final byte      EVENT_ROTATE   = 0x02;

    /** Record does not have a CRC computed. */
    public static final byte      CRC_TYPE_NONE  = 0x00;

    /** Record uses conventional CRC-32 computed by Java CRC32 class. */
    public static final byte      CRC_TYPE_32    = 0x01;

    private byte[]                data;
    private long                  offset;
    private byte                  crcType;
    private long                  crc;
    private boolean               truncated      = false;

    // Computed CRC from checkCRC() call.
    private long                  computedCrc    = -1;

    private ByteArrayInputStream  read;
    private ByteArrayOutputStream write;

    /**
     * Creates an empty record, which is optionally truncated.
     * 
     * @param offset File offset at which this record was read
     * @param truncated If true this record is truncated rather than merely
     *            empty
     */
    public LogRecord(long offset, boolean truncated)
    {
        this.offset = offset;
        this.data = null;
        this.crc = 0;
        this.crcType = CRC_TYPE_NONE;
        this.truncated = truncated;
    }

    /**
     * Creates a readable record with indicated content.
     * 
     * @param bytes Data in record
     * @param length Size of the record according to the log file
     * @param offset Starting offset in source log file
     * @param If true, the record is truncated, i.e., contains only partial
     *            record data
     */
    public LogRecord(long offset, byte[] bytes, byte crcType, long crc)
    {
        this.offset = offset;
        this.data = bytes;
        this.crcType = crcType;
        this.crc = crc;
        this.truncated = false;
    }

    /**
     * Returns the computed length of this record in the file, including length
     * field, data, and CRC.
     */
    public long getRecordLength()
    {
        if (data == null)
            return 0;
        else
            return data.length + NON_DATA_BYTES;
    }

    /**
     * Returns the offset into the source file of this record.
     */
    public long getOffset()
    {
        return offset;
    }

    /**
     * Returns the underlying byte buffer. Must call done() when writing before
     * calling this method.
     */
    public byte[] getData()
    {
        return data;
    }

    /**
     * Returns the CRC type.
     */
    public byte getCrcType()
    {
        return crcType;
    }

    /**
     * Returns the CRC value.
     */
    public long getCrc()
    {
        return crc;
    }

    /** Returns true if the record is truncated. */
    public boolean isTruncated()
    {
        return truncated;
    }

    /**
     * Returns true if the record is empty.
     */
    public boolean isEmpty()
    {
        return data == null;
    }

    /**
     * Compute and return CRC on data.
     */
    public long computeCrc() throws IOException
    {
        if (data == null || crcType == CRC_TYPE_NONE)
            computedCrc = 0;
        else if (crcType == CRC_TYPE_32)
        {
            computedCrc = computeCrc32(data);
        }
        else
        {
            throw new IOException("Invalid crc type: " + crcType);
        }

        return computedCrc;
    }

    /**
     * Compute and store CRC. This is used to populate CRC in a record to which
     * we are writing.
     */
    public void storeCrc(byte crcType) throws IOException
    {
        if (data == null || crcType == CRC_TYPE_NONE)
        {
            this.crc = 0;
            this.crcType = crcType;
        }
        else if (crcType == CRC_TYPE_32)
        {
            this.crc = computeCrc32(data);
            this.crcType = crcType;
        }
        else
        {
            throw new IOException("Invalid crc type: " + crcType);
        }
    }

    /**
     * Computes the CRC value and compares to the CRC stored in the record,
     * returning true only if the CRC values match.
     */
    public boolean checkCrc() throws IOException
    {
        if (computedCrc == -1)
            computeCrc();
        return computedCrc == crc;
    }

    /**
     * Static routine to compute CRC 32.
     */
    public static long computeCrc32(byte[] bytes) throws IOException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try
        {
            CheckedInputStream cis = new CheckedInputStream(bais, new CRC32());

            byte[] buf = new byte[128];
            while (cis.read(buf) >= 0)
            {
            }

            return cis.getChecksum().getValue();
        }
        finally
        {
            bais.close();
        }

    }

    /** Returns a stream to read record contents. */
    public InputStream read()
    {
        return new ByteArrayInputStream(data);
    }

    /** Returns a stream to write record contents. */
    public OutputStream write()
    {
        data = null;
        write = new ByteArrayOutputStream();
        return write;
    }

    /**
     * Deallocate resources and in the case of a writable log record write data.
     */
    public void done()
    {
        if (read != null)
        {
            try
            {
                read.close();
            }
            catch (IOException e)
            {
            }
            read = null;
        }
        if (write != null)
        {
            try
            {
                data = write.toByteArray();
                write.close();
            }
            catch (IOException e)
            {
            }
            write = null;
        }
    }

    /**
     * Print log record as string.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(": offset=").append(offset);
        if (data == null)
        {
            sb.append(" data=[] length=0");
        }
        else
        {
            sb.append(" data=");
            for (int i = 0; i < 10 && i < data.length; i++)
            {
                sb.append(String.format("%2X", data[i]));
            }
            if (data.length >= 10)
                sb.append("...");
            sb.append("] length=").append(data.length);
        }
        sb.append(" crcType=").append(crcType);
        sb.append(" crc=").append(crc);
        sb.append(" truncated=").append(truncated);

        return sb.toString();
    }

    /**
     * Return true if two records are equal, which means that offset, byte
     * array, and CRC all match.
     */
    public boolean equals(Object o)
    {
        if (!(o instanceof LogRecord))
            return false;
        LogRecord that = (LogRecord) o;
        if (offset != that.getOffset())
            return false;
        if (data == null)
        {
            if (that.getData() != null)
                return false;
        }
        else
        {
            if (data.length != that.getData().length)
                return false;
            for (int i = 0; i < data.length; i++)
            {
                if (data[i] != that.getData()[i])
                    return false;
            }
        }
        if (crcType != that.getCrcType())
            return false;
        if (crc != that.getCrc())
            return false;

        // Everything matches!
        return true;
    }
}