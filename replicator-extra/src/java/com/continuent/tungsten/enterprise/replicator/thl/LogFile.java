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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class manages I/O on a physical log file. It handles streams to read or
 * write from the underlying file.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LogFile
{
    static Logger              logger              = Logger.getLogger(LogFile.class);

    private static final int   MAGIC_NUMBER        = 0xC001CAFE;
    private static final short MAJOR_VERSION       = 0x0001;
    private static final short MINOR_VERSION       = 0x0001;
    private static final int   RECORD_LENGTH_SIZE  = 4;

    /**
     * Maximum value of a single record. Larger values indicate file corruption.
     */
    private static final int   MAX_RECORD_LENGTH   = 1000000000;

    /** Return immediately from write when there are no data. */
    public static final int    NO_WAIT             = 0;

    private File               file;
    private RandomAccessFile   randomFile;
    private boolean            writable;
    /** Fsync after this many milliseconds. 0 fsyncs after every write. */
    private long               fsyncIntervalMillis = 3000;
    private long               nextfsyncMillis     = 0;
    private long               baseSeqno;

    /**
     * Creates a file from a parent directory and child filename.
     */
    public LogFile(File parentDirectory, String fileName)
    {
        this.file = new File(parentDirectory, fileName);
    }

    /**
     * Creates a log file from a simple file.
     */
    public LogFile(File file)
    {
        this.file = file;
    }

    /**
     * Returns the log file.
     */
    public File getFile()
    {
        return file;
    }

    /**
     * Returns the number of milliseconds after which to fsync. 0 means fsync
     * after every record written.
     */
    public long getFsyncIntervalMillis()
    {
        return fsyncIntervalMillis;
    }

    public void setFsyncIntervalMillis(long fsyncIntervalMillis)
    {
        this.fsyncIntervalMillis = fsyncIntervalMillis;
    }

    /**
     * Returns a nicely formatting description of the file.
     */
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName()).append(": ");
        sb.append("name=").append(file.getName());
        if (randomFile == null)
        {
            sb.append(" open=n");
        }
        else
        {
            try
            {
                sb.append(" open=y available=").append(randomFile.length());
                sb.append(" offset=").append(randomFile.getFilePointer());
            }
            catch (IOException e)
            {
                sb.append(" [unable to get available/offset due to i/o error]");
            }
        }
        return sb.toString();
    }

    /**
     * Returns a sorted list of log files.
     * 
     * @param logDir Directory containing logs
     * @param logFilePrefix Prefix for log file names
     * @return Array of logfiles (zero-length if log is not initialized)
     */
    public static File[] listLogFiles(File logDir, String logFilePrefix)
    {
        // Find the log files and sort into file name order.
        ArrayList<File> logFiles = new ArrayList<File>();
        for (File f : logDir.listFiles())
        {
            if (!f.isDirectory() && f.getName().startsWith(logFilePrefix))
            {
                logFiles.add(f);
            }
        }
        File[] logFileArray = new File[logFiles.size()];
        return logFiles.toArray(logFileArray);
    }

    /**
     * Prepare the log file for reading. The file must exist.
     */
    public void prepareRead() throws THLException
    {
        // Set mode to readonly and open.
        writable = false;
        try
        {
            randomFile = new RandomAccessFile(file, "r");
        }
        catch (FileNotFoundException e)
        {
            throw new THLException("Unable to open file for reading: "
                    + file.getName(), e);
        }

        // Read the file header so we are correctly positioned in the file.
        this.checkFileHeader();
    }

    /**
     * Prepare the log file for writing. If the file does not exist we must
     * write a header with a base sequence number. NOTE: The file offset is
     * positioned after the header. A write will move it automatically to the
     * end. This allows start-up activities where we scan logs then optionally
     * write at the end, e.g., to repair a corrupt file.
     * 
     * @param seqno Base sequence number of this file (written to header)
     * @return True if the file was created
     */
    public boolean prepareWrite(long seqno) throws THLException
    {
        // Open the file, optionally creating same.
        boolean create = !file.exists();
        writable = true;
        try
        {
            randomFile = new RandomAccessFile(file, "rw");
        }
        catch (FileNotFoundException e)
        {
            throw new THLException("Failed to open file for writing: "
                    + file.getName(), e);
        }

        // If the file was created, write the header. Otherwise just read
        // the existing header.
        if (create)
        {
            baseSeqno = seqno;
            writeHeader(baseSeqno);
        }
        else
        {
            checkFileHeader();
        }

        return create;
    }

    /**
     * Flush and close file. It should be called after all other methods as part
     * of a clean shutdown.
     */
    public void release()
    {
        // Do not try to sync twice when releasing
        if (file != null)
            try
            {
                fsync();
                randomFile.close();
                randomFile = null;
                file = null;
            }
            catch (IOException e)
            {
                logger.warn(
                        "Unexpected I/O exception while closing log file: name="
                                + file.getName(), e);
            }
    }

    /**
     * Returns the base sequence number from the file header.
     */
    public long getBaseSeqno()
    {
        return baseSeqno;
    }

    /**
     * Returns the current position in the log file.
     */
    public long getOffset()
    {
        try
        {
            return randomFile.getFilePointer();
        }
        catch (IOException e)
        {
            logger.warn("Unable to determine log file offset: name="
                    + this.file.getAbsolutePath(), e);
            return -1;
        }
    }

    /**
     * Returns the length of the file.
     */
    public long getLength() throws IOException
    {
        return randomFile.length();
    }

    /**
     * Truncate the file to the provided length. Performs an automatic fsync.
     */
    public void setLength(long length) throws IOException
    {
        randomFile.setLength(length);
        fsync();
    }

    /**
     * Returns the bytes remaining to be read in this file from the current
     * position.
     */
    public long available() throws IOException
    {
        return randomFile.length() - randomFile.getFilePointer();
    }

    /**
     * Read the file header and return the log sequence number stored in the
     * file header.
     */
    private long checkFileHeader() throws THLException
    {
        int magic = 0;
        short major = 0;
        short minor = 0;

        try
        {
            magic = this.readInt();
            major = this.readShort();
            minor = this.readShort();
            baseSeqno = this.readLong();
        }
        catch (IOException e)
        {
            throw new THLException("Failed to read file header from  "
                    + file.getAbsolutePath(), e);
        }

        if (magic != MAGIC_NUMBER)
            throw new THLException("Could not open file "
                    + file.getAbsolutePath() + " : invalid magic number");
        if (major != MAJOR_VERSION)
            throw new THLException("Could not open file "
                    + file.getAbsolutePath() + " : incompatible major version");
        if (minor != MINOR_VERSION)
            logger.warn("Minor version mismatch : file "
                    + file.getAbsolutePath() + " using format " + major + "."
                    + minor + " - Tungsten running version " + MAJOR_VERSION
                    + "." + MINOR_VERSION);
        return baseSeqno;
    }

    /**
     * Write the new file header.
     * 
     * @param seqno Log sequence number of previous file (or -1).
     */
    public void writeHeader(long seqno) throws THLException
    {
        try
        {
            write(MAGIC_NUMBER);
            write(MAJOR_VERSION);
            write(MINOR_VERSION);
            write(seqno);
        }
        catch (IOException e)
        {
            throw new THLException("Unable to write file header: "
                    + file.getName(), e);
        }
    }

    /**
     * Reads a record from the file into a byte array. We may encounter a number
     * of unpredictable conditions at this point that we need to report
     * accurately to layers above us that will decide whether it represents a
     * problem.
     * 
     * @param waitMillis Number of milliseconds to wait for data to be
     *            available. 0 (NO_WAIT) means do not wait.
     * @returns A log record if we can read one before timing out
     * @throws IOException Thrown if there is an I/O error
     * @throws InterruptedException Thrown if we are interrupted
     * @throws LogTimeoutException Thrown if we timeout while waiting for data
     *             to appear
     */
    public LogRecord readRecord(int waitMillis) throws IOException,
            InterruptedException, LogTimeoutException
    {
        // See where we are and how much data is available.
        long offset = randomFile.getFilePointer();
        if (logger.isDebugEnabled())
            logger.debug("Reading log file position=" + offset + " available="
                    + available());

        // If there is nothing to read at this point and we don't want to
        // block, just return an empty record.
        if (waitMillis == NO_WAIT && available() == 0)
        {
            if (logger.isDebugEnabled())
                logger.debug("Read empty record");
            return new LogRecord(offset, false);
        }

        // If there is not enough to read the length, and we don't want to wait,
        // this is a truncated record.
        if (waitMillis == NO_WAIT && available() < RECORD_LENGTH_SIZE)
        {
            if (logger.isDebugEnabled())
                logger.debug("Length is truncated; returning immediately");
            return new LogRecord(offset, true);
        }

        // Set timeout. The waitMillis value is an integer so we assume
        // overflow is impossible.
        long timeoutMillis = System.currentTimeMillis() + waitMillis;

        // Wait until we see enough data to do a read or timeout.
        waitForData(RECORD_LENGTH_SIZE, timeoutMillis, offset);

        // Read the length. Check for corrupt data.
        int recordLength = randomFile.readInt();
        if (recordLength < LogRecord.NON_DATA_BYTES
                || recordLength > MAX_RECORD_LENGTH)
        {
            logger.warn("Record length is invalid, log may be corrupt: offset="
                    + offset + " record length=" + recordLength);
            return new LogRecord(offset, true);
        }

        if (logger.isDebugEnabled())
            logger.debug("Record length=" + recordLength);

        // If there is not enough to read and we don't want to wait, reset
        // the file pointer and return a truncated record.
        int remainingRecordLength = recordLength - RECORD_LENGTH_SIZE;
        if (waitMillis == NO_WAIT && available() < remainingRecordLength)
        {
            seekOffset(offset);
            return new LogRecord(offset, true);
        }

        // Wait until we get a full record or timeout.
        waitForData(remainingRecordLength, timeoutMillis, offset);

        // Finally, there's enough to read a record, so get it.
        byte[] bytesToRead = new byte[recordLength - LogRecord.NON_DATA_BYTES];
        randomFile.readFully(bytesToRead);
        byte crcType = randomFile.readByte();
        long crc = randomFile.readLong();
        return new LogRecord(offset, bytesToRead, crcType, crc);
    }

    // Wait until we have enough data to read or we timeout.
    private void waitForData(long recordLength, long timeoutMillis,
            long restoreOffset) throws IOException, InterruptedException,
            LogTimeoutException
    {
        // Wait until we see enough data to do a read or exceed the
        // timeout.
        while (available() < recordLength
                && System.currentTimeMillis() < timeoutMillis)
        {
            Thread.sleep(500);
            if (logger.isDebugEnabled())
                logger.debug("Sleeping for 500 ms");
        }

        // Check for timeout. If so, we need to restore the offset
        // and return.
        long available = available();
        if (available < RECORD_LENGTH_SIZE)
        {
            if (logger.isDebugEnabled())
                logger.debug("Timed out waiting for length to appear");
            seekOffset(restoreOffset);
            throw new LogTimeoutException("Log read timeout: file="
                    + file.getName() + " offset=" + restoreOffset);
        }
    }

    /**
     * Writes a buffer to the log file and returns true if we have exceeded the
     * log file size.
     * 
     * @param barr Array containing data to write
     * @param logFileSize Maximum log file size
     */
    public boolean writeRecord(LogRecord record, int logFileSize)
            throws IOException, InterruptedException
    {
        // Write the length followed by the code.
        assertWritable();
        randomFile.writeInt((int) record.getRecordLength());
        randomFile.write(record.getData());
        randomFile.writeByte(record.getCrcType());
        randomFile.writeLong(record.getCrc());

        // Fsync if we have exceeded the fsync interval.
        if (System.currentTimeMillis() >= nextfsyncMillis)
        {
            fsync();
            nextfsyncMillis = System.currentTimeMillis()
                    + this.fsyncIntervalMillis;
        }

        // Size of log files is now a number of bytes
        if (logFileSize > 0 && randomFile.length() > logFileSize)
            return true;
        else
            return false;
    }

    /** Read a single integer. */
    protected int readInt() throws IOException
    {
        if (available() < 4)
            throw new IOException(
                    "Unable to read an integer from the file : expecting 4 bytes, but only "
                            + available() + "byte(s) available");
        return randomFile.readInt();
    }

    /** Reads a single short. */
    protected short readShort() throws IOException
    {
        if (available() < 2)
            throw new IOException(
                    "Unable to read an integer from the file : expecting 2 bytes, but only "
                            + available() + "byte(s) available");
        return randomFile.readShort();
    }

    /** Reads a single long. */
    protected long readLong() throws IOException
    {
        if (available() < 8)
            throw new IOException(
                    "Unable to read a long from the file : expecting 8 bytes, but only "
                            + available() + "byte(s) available");
        return randomFile.readLong();
    }

    /**
     * Writes a UTF-8 special encoded string.
     */
    protected void write(String string) throws IOException
    {
        assertWritable();
        randomFile.writeUTF(string);
    }

    protected void write(int myInt) throws IOException
    {
        assertWritable();
        randomFile.writeInt(myInt);
    }

    protected void write(long seqno) throws IOException
    {
        assertWritable();
        randomFile.writeLong(seqno);
    }

    protected void write(short myShort) throws IOException
    {
        assertWritable();
        randomFile.writeShort(myShort);
    }

    /**
     * Synchronizes file contents to disk using fsync (or Java equivalent). You
     * must call this method to commit data.
     */
    public void fsync() throws IOException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Issuing log file fsync");
        }
        randomFile.getFD().sync();
    }

    /**
     * Seeks to a particular offset in the file.
     * 
     * @throws IOException If positioning results in an error.
     */
    public void seekOffset(long offset) throws IOException
    {
        randomFile.seek(offset);
        if (logger.isDebugEnabled())
        {
            logger.debug("Skipping to position " + offset + " into file "
                    + this.file.getName());
        }
    }

    /**
     * Seeks to the end of the file.
     * 
     * @throws IOException If positioning results in an error.
     */
    public void seekToEnd() throws IOException
    {
        long end = randomFile.length();
        randomFile.seek(end);
        if (logger.isDebugEnabled())
        {
            logger.debug("Skipping to end offset " + end + " into file "
                    + this.file.getName());
        }
    }

    // Provide routine to ensure current file able to write. This seeks to the
    // end
    // of the file in preparation for writing.
    private void assertWritable() throws IOException
    {
        if (!writable)
            throw new IOException("Attempt to update non-writable file: name="
                    + file.getAbsolutePath() + " offset="
                    + randomFile.getFilePointer());
        seekToEnd();
    }
}
