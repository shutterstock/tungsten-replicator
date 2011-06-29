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

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * Test capabilities for buffered writes to files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BufferedFileDataOutputTest extends TestCase
{
    private static Logger logger = Logger
                                         .getLogger(BufferedFileDataOutputTest.class);

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
     * Confirm that we can open non-existing and existing files.
     */
    public void testOpen() throws Exception
    {
        File f = initFile("testOpen");
        assertFalse("File does not exist", f.exists());

        BufferedFileDataOutput bfdo = new BufferedFileDataOutput(f);
        bfdo.close();
        assertTrue("File does exist", f.exists());

        BufferedFileDataOutput bfdo2 = new BufferedFileDataOutput(f);
        bfdo2.close();
    }

    /**
     * Confirm that we can write to new and existing files.
     */
    public void testBasicWrite() throws Exception
    {
        File f = initFile("testBasicWrite");

        // Write int to new file.
        BufferedFileDataOutput bfdo = new BufferedFileDataOutput(f);
        bfdo.writeInt(1);
        assertEquals("Offset after 1 int", 4, bfdo.getOffset());
        bfdo.fsync();
        assertEquals("File size matches offset after fsync", 4, f.length());
        bfdo.close();

        // Write a second int.
        BufferedFileDataOutput bfdo2 = new BufferedFileDataOutput(f);
        bfdo2.writeInt(2);
        assertEquals("Offset after 2 ints", 8, bfdo2.getOffset());
        bfdo2.fsync();
        assertEquals("File size matches offset after fsync", 8, f.length());
        bfdo2.close();

        // Confirm both values made it to the file.
        assertEquals("File length after writing", 8, f.length());
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        assertEquals("int 1", 1, bfdi.readInt());
        assertEquals("int 2", 2, bfdi.readInt());
        bfdi.close();
    }

    /**
     * Confirm that we can open a file for writing and correctly writes standard
     * byte, short, int, long, and byte array values. This is a very boring test
     * but very helpful.
     */
    public void testWrite() throws Exception
    {
        File f = this.initFile("testWrite");
        BufferedFileDataOutput bfdo = new BufferedFileDataOutput(f);

        // Bytes.
        bfdo.writeByte((byte) 1);
        bfdo.writeByte(Byte.MAX_VALUE);
        bfdo.writeByte(Byte.MIN_VALUE);
        assertEquals("Size after bytes", 3, bfdo.getOffset());

        // Shorts.
        bfdo.writeShort((short) 2);
        bfdo.writeShort(Short.MAX_VALUE);
        bfdo.writeShort(Short.MIN_VALUE);
        assertEquals("Size after shorts", 9, bfdo.getOffset());

        // Ints.
        bfdo.writeInt(3);
        bfdo.writeInt(Integer.MAX_VALUE);
        bfdo.writeInt(Integer.MIN_VALUE);
        assertEquals("Size after ints", 21, bfdo.getOffset());

        // Longs.
        bfdo.writeLong(4);
        bfdo.writeLong(Long.MAX_VALUE);
        bfdo.writeLong(Long.MIN_VALUE);
        assertEquals("Size after longs", 45, bfdo.getOffset());

        // Byte arrays.
        byte[] byteArray = new byte[10];
        for (int i = 0; i < byteArray.length; i++)
            byteArray[i] = (byte) i;
        bfdo.write(byteArray);
        assertEquals("Size after byte array", 55, bfdo.getOffset());
        bfdo.fsync();
        bfdo.close();

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
     * Confirm that we can truncate and then rewrite part of a file.
     */
    public void testTruncate() throws Exception
    {
        // Write a test file with 100 values.
        File f = this.initFile("testTruncate");
        BufferedFileDataOutput bfdo = new BufferedFileDataOutput(f);
        for (int i = 0; i < 100; i++)
        {
            bfdo.writeInt(i);
        }
        bfdo.fsync();

        // Truncate off the last 50 ints one by one.
        for (int i = 99; i >= 50; i--)
        {
            int length = i * 4;
            bfdo.setLength(length);
            assertEquals("truncated file length", length, f.length());
        }

        // Write missing 50 ints.
        for (int i = 50; i < 100; i++)
        {
            bfdo.writeInt(i);
        }
        bfdo.fsync();

        // Read and confirm the contents of the file.
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f);
        for (int i = 0; i < 100; i++)
        {
            assertEquals("i: " + i, i, bfdi.readInt());
        }

        // Clean up.
        bfdi.close();
        bfdo.close();
    }

    /**
     * Confirm that we can truncate and then rewrite part of a file.
     */
    public void testLargeScaleWrite() throws Exception
    {
        // Write a 20Mb file with period fsync operations.
        File f = this.initFile("testLargeScaleWrite");
        BufferedFileDataOutput bfdo = new BufferedFileDataOutput(f, 65536);
        for (int i = 0; i < 5000000; i++)
        {
            bfdo.writeInt(i);
            if (i % 20000 == 0)
                bfdo.fsync();
            int bytesWritten = (i + 1) * 4;
            if (bytesWritten % 1000000 == 0)
                logger.info("Bytes written: " + bytesWritten);
        }
        bfdo.writeByte((byte) -1);
        bfdo.fsync();
        bfdo.close();
        assertEquals("File size", (5000000 * 4) + 1, f.length());

        // Read and confirm the contents of the file.
        BufferedFileDataInput bfdi = new BufferedFileDataInput(f, 65536);
        for (int i = 0; i < 5000000; i++)
        {
            assertEquals("i: " + i, i, bfdi.readInt());
        }
        assertEquals("Last byte", (byte) -1, bfdi.readByte());

        // Clean up.
        bfdi.close();
    }

    // Initialize a test file by clearing and return the File instance.
    private File initFile(String name)
    {
        File f = new File(name);
        if (f.exists())
            f.delete();
        return f;
    }
}