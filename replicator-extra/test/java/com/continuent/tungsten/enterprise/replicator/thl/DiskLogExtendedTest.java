/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
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

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.File;
import java.sql.Timestamp;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Tests concurrency operations on the disk log. This test is an extension of
 * cases in DiskLogTest and uses a similar approach.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DiskLogExtendedTest extends TestCase
{
    private static Logger logger = Logger.getLogger(DiskLogExtendedTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
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
     * Confirm that we can handle multiple readers accessing the log at
     * different locations while simultaneously writing. We need to make the
     * logs small enough to pick off problems with log rotate events not
     * appearing in time.
     */
    public void testConcurrentAccess() throws Exception
    {
        // Create the log and write multiple events.
        File logDir = prepareLogDir("testConcurrentAccess");
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setReadOnly(false);
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(10000000);
        log.setFlushIntervalMillis(100);
        log.prepare();

        // Write a bunch of events to the log.
        writeEventsToLog(log, 0, 10000);

        // Create and start readers.
        HashMap<Thread, SimpleLogReader> tasks = new HashMap<Thread, SimpleLogReader>();
        for (int i = 0; i < 10; i++)
        {
            SimpleLogReader reader = new SimpleLogReader(log, 0, 10000);
            Thread thread = new Thread(reader);
            tasks.put(thread, reader);
            thread.start();
        }

        // Wait for readers to finish. 30 seconds should be sufficient.
        for (Thread thread : tasks.keySet())
        {
            thread.join(30000);
            thread.interrupt();

            SimpleLogReader reader = tasks.get(thread);
            if (reader.error != null)
            {
                throw new Exception(
                        "Reader thread failed with exception after "
                                + reader.eventsRead + " events", reader.error);
            }
            assertEquals("Checking events read", 10000, reader.eventsRead);
        }

        // Release the log.
        log.release();
    }

    /**
     * Test the effect of varying fsync intervals from 20 to 2000ms and using
     * buffer sizes from 64k to 512k.
     */
    public void testVaryingFsync() throws Exception
    {
        // Define fsync intervals and buffer sizes for test.
        long[] fsyncIntervals = {20, 100, 500, 2000};
        int[] bufferSizes = {65536, 524288};

        // Perform a test for each interval.
        int run = 0;
        for (long fsync : fsyncIntervals)
        {
            for (int buffer : bufferSizes)
            {
                run++;

                // Initialize log.
                File logDir = prepareLogDir("testVaryingFsync");
                DiskLog log = new DiskLog();
                log.setDoChecksum(false);
                log.setReadOnly(false);
                log.setLogDir(logDir.getAbsolutePath());
                log.setLogFileSize(100000000);
                log.setFlushIntervalMillis(fsync);
                log.setBufferSize(buffer);

                // Write a bunch of events to the log.
                long start = System.currentTimeMillis();
                log.prepare();
                writeEventsToLog(log, 0, 1000000);
                log.release();
                long end = System.currentTimeMillis();

                // Print elapsed time.
                double elapsed = (end - start) / 1000.0;
                String msg = String.format(
                        "Test %d: fsync ms=%d, buffer=%d, elapsed secs=%f\n",
                        run, fsync, buffer, elapsed);
                logger.info(msg);
            }
        }
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName)
    {
        File logDir = new File(logDirName);
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
        }
        else
        {
            logDir.mkdirs();
        }
        return logDir;
    }

    // Write a prescribed number of events to the log starting at a
    // specified sequence number. Last event will be committed.
    private void writeEventsToLog(DiskLog log, long seqno, int howMany)
            throws THLException, InterruptedException
    {
        // Should match incremented seqno on the final iteration.
        long lastSeqno = seqno + howMany;

        // Write a series of events to the log.
        for (int i = 0; i < howMany; i++)
        {
            THLEvent e = this.createTHLEvent(seqno++);
            log.store(e, (seqno == lastSeqno));
        }
        assertEquals("Should have stored requested events", (seqno - 1), log
                .getMaxSeqno());
        logger.info("Final seqno: " + (seqno - 1));
    }

    // Create a dummy THL event.
    private THLEvent createTHLEvent(long seqno, short fragno, boolean lastFrag,
            String sourceId)
    {
        String eventId = new Long(seqno).toString();
        ReplDBMSEvent replEvent = new ReplDBMSEvent(seqno, fragno, lastFrag,
                sourceId, 1, new Timestamp(System.currentTimeMillis()),
                new DBMSEvent());
        return new THLEvent(eventId, replEvent);
    }

    private THLEvent createTHLEvent(long seqno)
    {
        return createTHLEvent(seqno, (short) 0, true, "test");
    }
}