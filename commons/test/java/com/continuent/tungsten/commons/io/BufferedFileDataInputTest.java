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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Test capabilities for buffered reads and writes to files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BufferedFileDataInputTest extends TestCase
{
    private static Logger logger = Logger
                                         .getLogger(BufferedFileDataInputTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        logger.info("Test starting");
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Confirm that we can open and close an existing 0 length input file.
     */
    public void testInputOpen() throws Exception
    {
        File f = initFile("testInputOpen");
        FileOutputStream fos = new FileOutputStream(f);
        fos.close();

        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        assertEquals("Should be at offset 0", 0, bfdi.getOffset());
        bfdi.close();
    }

    /**
     * Confirm that opening a file for reading fails if the file does not exist.
     */
    public void testInputNonexistent() throws Exception
    {
        File f = new File("testInputNonExistent");
        try
        {
            new BufferedFileDataInput(f);
            throw new Exception("Able to open non-existent file: "
                    + f.getAbsolutePath());
        }
        catch (FileNotFoundException e)
        {
        }
    }

    /**
     * Confirm that we can open an existing file for reading and correctly
     * extract standard byte, short, int, long, and byte array values. This is a
     * very boring test but very helpful.
     */
    public void testInputRead() throws Exception
    {
        File f = this.initFile("testInputRead");
        FileOutputStream fos = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(fos);

        // Bytes.
        dos.writeByte(1);
        dos.writeByte(Byte.MAX_VALUE);
        dos.writeByte(Byte.MIN_VALUE);

        // Shorts.
        dos.writeShort(2);
        dos.writeShort(Short.MAX_VALUE);
        dos.writeShort(Short.MIN_VALUE);

        // Ints.
        dos.writeInt(3);
        dos.writeInt(Integer.MAX_VALUE);
        dos.writeInt(Integer.MIN_VALUE);

        // Longs.
        dos.writeLong(4);
        dos.writeLong(Long.MAX_VALUE);
        dos.writeLong(Long.MIN_VALUE);

        // Byte arrays.
        byte[] byteArray = new byte[10];
        for (int i = 0; i < byteArray.length; i++)
            byteArray[i] = (byte) i;
        dos.write(byteArray);
        dos.flush();
        dos.close();

        // Fire up a reader instance.
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);

        // Read bytes;
        assertEquals("byte 1", 1, bfdi.readByte());
        assertEquals("byte 2", Byte.MAX_VALUE, bfdi.readByte());
        assertEquals("byte 3", Byte.MIN_VALUE, bfdi.readByte());
        assertEquals("Should be at offset 3", 3, bfdi.getOffset());

        // Read shorts.
        assertEquals("short 1", 2, bfdi.readShort());
        assertEquals("short 2", Short.MAX_VALUE, bfdi.readShort());
        assertEquals("short 3", Short.MIN_VALUE, bfdi.readShort());
        assertEquals("Should be at offset 9", 9, bfdi.getOffset());

        // Read ints.
        assertEquals("int 1", 3, bfdi.readInt());
        assertEquals("int 2", Integer.MAX_VALUE, bfdi.readInt());
        assertEquals("int 3", Integer.MIN_VALUE, bfdi.readInt());
        assertEquals("Should be at offset 21", 21, bfdi.getOffset());

        // Read longs.
        assertEquals("long 1", 4, bfdi.readLong());
        assertEquals("long 2", Long.MAX_VALUE, bfdi.readLong());
        assertEquals("long 3", Long.MIN_VALUE, bfdi.readLong());
        assertEquals("Should be at offset 45", 45, bfdi.getOffset());

        // Read bytes.
        byte[] myBytes = new byte[10];
        bfdi.readFully(myBytes);
        for (int i = 0; i < byteArray.length; i++)
        {
            assertEquals("byte: " + i, byteArray[i], myBytes[i]);
        }
        assertEquals("Should be at offset 55", 55, bfdi.getOffset());

        // Clean up.
        bfdi.close();
    }

    /**
     * Confirm that we can open and either seek or skip to known locations in a
     * file.
     */
    public void testInputSeek() throws Exception
    {
        // Write a test file with 5M int values.
        int size = 5000000;
        File f = this.initFile("testInputResources");
        writeAscendingIntFile(f, size);

        // Compute number of jumps and the size of each block in a jump.
        int jumps = size / 5000;
        int jumpSize = size / jumps;
        int jumpOffset = jumpSize * 4;

        // Test our ability to seek forward to arbitrary positions in the file.
        logger.info("Seeking forward...");
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        for (int i = 0; i < jumps; i++)
        {
            // Compute expected value and file offset.
            int value = i * jumpSize;
            long offset = i * jumpOffset;

            bfdi.seek(offset);
            String position = "i: " + i;
            assertEquals(position, offset, bfdi.getOffset());
            assertEquals(position, value, bfdi.readInt());
        }
        bfdi.close();

        // Now test our ability to seek backwards in the file.
        logger.info("Seeking backward...");
        bfdi = new BufferedFileDataInput(f);
        for (int i = (jumps - 1); i >= 0; i--)
        {
            // Compute expected value and file offset.
            int value = i * jumpSize;
            long offset = i * jumpOffset;

            bfdi.seek(offset);
            String position = "i: " + i;
            assertEquals(position, offset, bfdi.getOffset());
            assertEquals(position, value, bfdi.readInt());
        }
        bfdi.close();

        // Clean up to eliminate large files.
        f.delete();
    }

    /**
     * Confirm that we can use mark/reset to perform trial reads, then return to
     * original position.
     */
    public void testInputMarkReset() throws Exception
    {
        // Write a test file with 10000 int values.
        int size = 10000;
        File f = this.initFile("testInputMarkReset");
        writeAscendingIntFile(f, size);

        // Test our ability read, reset, and read again.
        int jumps = size / 100;
        int blockSize = size / jumps;
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        for (int j = 0; j < jumps; j++)
        {
            // Perform initial read.
            bfdi.mark(blockSize * 4);
            int nextInt = j * blockSize;
            for (int i = 0; i < blockSize; i++)
            {
                String position = "block: " + j + " i: " + i;
                assertEquals(position, nextInt++, bfdi.readInt());
            }

            // Reset and reread.
            bfdi.reset();
            nextInt = j * blockSize;
            for (int i = 0; i < blockSize; i++)
            {
                String position = "block: " + j + " i: " + i;
                assertEquals(position, nextInt++, bfdi.readInt());
            }

            if ((j + 1) % 50 == 0)
                logger.info("Mark/reset intervals: " + (j + 1));
        }
        bfdi.close();
    }

    /**
     * Confirm that we can read without blocking by checking the number of bytes
     * available. We do this by writing, then reading from a file in a loop.
     */
    public void testInputAvailable() throws Exception
    {
        // Set up writes to file as well as input reader.
        File f = this.initFile("testInputAvailable");
        FileOutputStream fos = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(fos);

        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);

        // Write and then read each value.
        for (int i = 0; i < 100; i++)
        {
            // Write value and flush to disk.
            dos.writeInt(i);
            dos.flush();

            // Ensure value is available, then read.
            assertEquals("available bytes: " + i, 4, bfdi.available());
            assertEquals("value of int: " + i, i, bfdi.readInt());
        }

        // Clean up and go home.
        dos.close();
        bfdi.close();
    }

    /**
     * Confirm that for a short file available bytes are equal to the size of
     * the file and that available bytes decrease as we read items of various
     * sizes.
     */
    public void testInputAvailable2() throws Exception
    {
        File f = this.initFile("testInputAvailable2");
        FileOutputStream fos = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(fos);

        // Fire up a reader instance.
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        assertEquals("initial availability", 0, bfdi.available());

        // Write and check a byte.
        dos.writeByte(1);
        dos.flush();
        assertEquals("after byte availability", 1, bfdi.available());

        // Short.
        dos.writeShort(2);
        dos.flush();
        assertEquals("after short availability", 3, bfdi.available());

        // Int.
        dos.writeInt(3);
        dos.flush();
        assertEquals("after int availability", 7, bfdi.available());

        // Longs.
        dos.writeLong(4);
        dos.flush();
        assertEquals("after long availability", 15, bfdi.available());

        // Byte arrays.
        byte[] byteArray = new byte[10];
        for (int i = 0; i < byteArray.length; i++)
            byteArray[i] = (byte) i;
        dos.write(byteArray);
        dos.flush();
        assertEquals("after byte array availability", 25, bfdi.available());
        dos.close();

        // Read bytes;
        assertEquals("byte 1", 1, bfdi.readByte());
        assertEquals("after byte read", 24, bfdi.available());

        // Read shorts.
        assertEquals("short 1", 2, bfdi.readShort());
        assertEquals("after short read", 22, bfdi.available());

        // Read ints.
        assertEquals("int 1", 3, bfdi.readInt());
        assertEquals("after int read", 18, bfdi.available());

        // Read longs.
        assertEquals("long 1", 4, bfdi.readLong());
        assertEquals("after int read", 10, bfdi.available());

        // Read bytes.
        byte[] myBytes = new byte[10];
        bfdi.readFully(myBytes);
        assertEquals("after byte array read", 0, bfdi.available());

        // Clean up.
        bfdi.close();
    }

    /**
     * Confirm that we can wait for enough output to be provided in order to do
     * a non-blocking read and that the wait fails if sufficient data do not
     * appear.
     */
    public void testInputWaitAvailable() throws Exception
    {
        File f = this.initFile("testInputWaitAvailable");
        FileOutputStream fos = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(fos);

        // Confirm wait returns 0 on empty file. 
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        assertEquals("empty file", 0, bfdi.waitAvailable(4, 10));

        // Wait when more than enough data. 
        dos.writeInt(0);
        dos.writeInt(1);
        dos.flush();
        assertEquals("sufficient data", 8, bfdi.waitAvailable(4, 10));
        
        // Wait when exactly enough data. 
        bfdi.readInt();
        assertEquals("exactly enough data", 4, bfdi.waitAvailable(4, 10));

        // Wait when data exhausted. 
        bfdi.readInt();
        assertEquals("data exhausted", 0, bfdi.waitAvailable(4, 10));

        // Clean up.
        dos.close();
        bfdi.close();
    }

    /**
     * Verify that we can skip forwards using a couple of different buffer
     * sizes.
     * 
     * @throws Exception
     */
    public void testInputSkipWithBuffering() throws Exception
    {
        // Write a test file with 1M int values.
        int size = 1000000;
        File f = this.initFile("testInputResources");
        writeAscendingIntFile(f, size);

        // Compute number of jumps and the size of each block in a jump.
        int jumps = size / 5000;
        int jumpSize = size / jumps;
        int jumpOffset = jumpSize * 4;

        // Skip through the file with two different buffer sizes.
        int bsize = 256;
        for (int b = 0; b < 2; b++)
        {
            // Create reader.
            logger.info("Reading with buffer size=" + bsize);
            BufferedFileDataInput bfdi = new BufferedFileDataInput(f, bsize);
            bsize *= 256;

            // Loop across the values.
            for (int i = 0; i < jumps; i++)
            {

                // Compute expected value and file offset.
                int value = i * jumpSize;
                long offset = i * jumpOffset;

                bfdi.seek(offset);
                String position = "i: " + i;
                assertEquals(position, offset, bfdi.getOffset());
                assertEquals(position, value, bfdi.readInt());

                // Now skip forward.
                bfdi.skip(jumpOffset);
            }

            // Release resources.
            logger.info(bfdi);
            bfdi.close();
        }
    }

    /**
     * Confirm that we can re-read the same file thousands of times without
     * triggering a resource leak, e.g., of file descriptors.
     */
    public void testInputResources() throws Exception
    {
        // Write a test file with 100 int values.
        File f = this.initFile("testInputResources");
        writeAscendingIntFile(f, 100);

        // Open and read the file 25000 times.
        for (int fcnt = 0; fcnt < 25000; fcnt++)
        {
            BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
            long offset = bfdi.getOffset();
            for (int i = 0; i < 100; i++)
            {
                String position = "fcnt: " + fcnt + " i: " + i;
                assertEquals(position, i, bfdi.readInt());
                offset += 4;
                assertEquals(position, offset, bfdi.getOffset());
            }
            bfdi.close();
        }
    }

    // Initialize a test file by clearing and return the File instance.
    private File initFile(String name)
    {
        File f = new File(name);
        if (f.exists())
            f.delete();
        return f;
    }

    // Writes a file filled with ascending int values up to a specified value.
    // The resulting file is 4 * n bytes long. The last int value is n - 1.
    private void writeAscendingIntFile(File f, int n) throws IOException
    {
        logger.info("Writing data file: name=" + f.getAbsolutePath() + " n="
                + n);
        FileOutputStream fos = new FileOutputStream(f);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        DataOutputStream dos = new DataOutputStream(bos);

        for (int i = 0; i < n; i++)
            dos.writeInt(i);
        dos.close();
    }
}