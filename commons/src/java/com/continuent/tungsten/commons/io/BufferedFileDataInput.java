/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.commons.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * Merges the capabilities of the following stream classes into a single class:
 * FileInputStream, BufferedInputStream, and DataInputStream. This allows us to
 * manage buffered data reads from files efficiently.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class BufferedFileDataInput
{
    private static Logger       logger = Logger.getLogger(BufferedFileDataInput.class);
    // Read parameters.
    private File                file;
    private int                 size;

    // Variables to control reading.
    private FileInputStream     fileInput;
    private long                offset;
    private long                markOffset;
    private long                available;
    private FileChannel         fileChannel;

    /**
     * Creates instance positioned on start of file.
     * 
     * @param file File from which to read
     * @param size Size of buffer for buffered I/O
     */
    public BufferedFileDataInput(File file, int size) throws FileNotFoundException,
            IOException, InterruptedException
    {
        this.file = file;
        this.size = size;
        seek(0);
    }

    /**
     * Creates instance with default buffer size.
     */
    public BufferedFileDataInput(File file) throws FileNotFoundException,
            IOException, InterruptedException
    {
        this(file, 1024);
    }

    /**
     * Returns the current offset position.
     */
    public long getOffset()
    {
        return offset;
    }

    /**
     * Query the stream directly for the number of bytes available for immediate
     * read without blocking. This operation may result in a file system
     * metadata call. To find out if a specific number of bytes are known to be
     * available use waitForAvailable().
     * 
     * @return Number of bytes available for non-blocking read
     */
    public long available() throws IOException
    {
        available = fileChannel.size() - offset;
        return available;
    }

    /**
     * Waits for a specified number of bytes to be available for a non-blocking
     * read.
     * 
     * @param requested Number of bytes to read
     * @param waitMillis Milliseconds to wait before timeout
     * @return Number of bytes available for non-blocking read
     * @throws IOException Thrown if there is a problem checking for available
     *             bytes
     * @throws InterruptedException Thrown if we are interrupted while waiting
     */
    public long waitAvailable(int requested, int waitMillis)
            throws IOException, InterruptedException
    {
        // If we know there is already enough data to read, return immediately.
        if (available >= requested)
            return available;

        // Since there is not enough, wait until we see enough data to do a read
        // or exceed the timeout.
        long timeoutMillis = System.currentTimeMillis() + waitMillis;
        while (available() < requested
                && System.currentTimeMillis() < timeoutMillis)
        {
            Thread.sleep(500);
            if (logger.isDebugEnabled())
                logger.debug("Sleeping for 500 ms");
        }

        // Return true or false depending on whether we found the data.
        return available;
    }

    /**
     * Mark stream to read up to limit.
     * 
     * @param readLimit Number of bytes that may be read before resetting
     */
    public void mark(int readLimit)
    {
        markOffset = offset;
    }

    /**
     * Reset stream back to last mark.
     * 
     * @throws IOException Thrown if mark has been invalidated or not set
     * @throws InterruptedException Thrown if we are interrupted
     */
    public void reset() throws IOException, InterruptedException
    {
        try
        {
            if (markOffset >= 0)
                fileChannel.position(markOffset);
            offset = markOffset;
        }
        catch (IOException e)
        {
            // Need to seek directly as mark is invalidated.
            this.seek(markOffset);
        }
        markOffset = -1;
    }

    /**
     * Skip requested number of bytes.
     * 
     * @param bytes Number of bytes to skip
     * @return Number of bytes actually skipped
     * @throws IOException Thrown if seek not supported or other error
     */
    public long skip(long bytes) throws IOException
    {
        offset += bytes;
        fileChannel.position(offset);
        available -= bytes;
        return bytes;
    }

    /**
     * Seek to a specific offset in the file.
     * 
     * @param seekBytes Number of bytes from start of file
     * @throws IOException Thrown if offset cannot be found
     * @throws FileNotFoundException Thrown if file is not found
     * @throws InterruptedException Thrown if thread is interrupted
     */
    public void seek(long seekBytes) throws FileNotFoundException, IOException,
            InterruptedException
    {
        if (fileChannel != null)
            fileChannel.close();

        if (fileInput != null)
            fileInput.close();

        fileInput = new FileInputStream(file);
        fileChannel = fileInput.getChannel();

        try
        {
            fileChannel.position(seekBytes);
        }
        catch (ClosedByInterruptException e)
        {
            // NIO rewrites InterruptException into this, which seems broken.
            // To preserve interrupt handling behavior up the stack, we throw
            // InterruptException.
            throw new InterruptedException();
        }
        offset = seekBytes;
        markOffset = -1;
        available = 0;
    }

    /**
     * Reads a single byte.
     */
    public byte readByte() throws IOException
    {
        ByteBuffer bbuf = ByteBuffer.allocate(1);
        fileChannel.read(bbuf);
        bbuf.flip();

        offset += 1;
        available -= 1;
        return bbuf.get();
    }

    /** Reads a single short. */
    public short readShort() throws IOException
    {
        ByteBuffer bbuf = ByteBuffer.allocate(2);
        fileChannel.read(bbuf);
        bbuf.flip();

        short v = bbuf.getShort();
        offset += 2;
        available -= 2;
        return v;
    }

    /** Read a single integer. */
    public int readInt() throws IOException
    {
        ByteBuffer bbuf = ByteBuffer.allocate(4);
        fileChannel.read(bbuf);
        bbuf.flip();

        offset += 4;
        available -= 4;
        return bbuf.getInt();
    }

    /** Reads a single long. */
    public long readLong() throws IOException
    {
        ByteBuffer bbuf = ByteBuffer.allocate(8);
        fileChannel.read(bbuf);
        bbuf.flip();

        long v = bbuf.getLong();
        offset += 8;
        available -= 8;
        return v;
    }

    /**
     * Reads a full byte array completely.
     * 
     * @throws IOException Thrown if full byte array cannot be read
     */
    public void readFully(byte[] bytes) throws IOException
    {
        readFully(bytes, 0, bytes.length);
    }

    /**
     * Reads a full byte array completely.
     * 
     * @param buf Buffer into which to read
     * @param start Starting byte position
     * @param len Number of bytes to read
     * @throws IOException Thrown if data cannot be read
     */
    public void readFully(byte[] bytes, int start, int len) throws IOException
    {
        ByteBuffer bbuf = ByteBuffer.wrap(bytes, start, len);
        int length = fileChannel.read(bbuf);
        if (length > 0)
        {
            offset += length;
            available -= length;
        }
    }

    /** Close and release all resources. */
    public void close()
    {
        try
        {
            if (fileChannel != null)
                fileChannel.close();
            fileInput.close();
        }
        catch (IOException e)
        {
            logger.warn("Unable to close buffered file reader: file="
                    + file.getName() + " exception=" + e.getMessage());
        }
        fileInput = null;
        offset = -1;
        available = 0;
    }

    /**
     * Print contents of the reader.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" file=").append(file.getName());
        sb.append(" size=").append(size);
        sb.append(" offset=").append(offset);
        return sb.toString();
    }
}