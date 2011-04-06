/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * Contact: tungsten@continuent.com
 *
 * This program is property of Continuent.  All rights reserved. 
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.enterprise.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Tests public methods on the disk log. The tests in this suite require a
 * directory in which to write log records. The default serializer for this test
 * is currently the ProtobufSerializer. To use the Java serializer define the
 * following macro: <code><pre>
 *   -Dserializer=com.continuent.tungsten.enterprise.replicator.thl.serializer.JavaSerializer
 * </pre></code> The serializer class will be loaded.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DiskLogTest extends TestCase
{
    private static Logger logger     = Logger.getLogger(DiskLogTest.class);
    // private Class<?> serializer = JavaSerializer.class;
    private Class<?>      serializer = ProtobufSerializer.class;

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        String serializerClass = System.getProperty("serializer");
        if (serializerClass != null)
        {
            serializer = Class.forName(serializerClass);
        }
        logger.info("Using serializer: " + serializer.getName());
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
     * Confirm that we can prepare and release the log.
     */
    public void testLogPreparation() throws Exception
    {
        File logDir = new File("testLogPreparation");
        prepareLogDir(logDir);
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializer(ProtobufSerializer.class.getName());
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(1000000);

        log.prepare();
        assertEquals("Max sequence number is -1 for empty log", -1, log
                .getMaxSeqno());
        log.validate();
        log.release();
    }

    /**
     * Confirm that log correctly identifies the max sequence number when
     * starting new log with sequence number above 0.
     */
    public void testLogNonZeroStart() throws Exception
    {
        // Create the log and write an event that starts with seqno > 0.
        DiskLog log = openLog("testLogNonZeroStart", true);
        writeEventsToLog(log, 100, 1);
        assertEquals("Should show correct max seqno from single record", 100,
                log.getMaxSeqno());
        log.release();

        // Reopen the log and read back.
        DiskLog log2 = openLog("testLogNonZeroStart", false);
        log.validate();
        assertEquals("Should show correct max seqno from single record", 100,
                log.getMaxSeqno());
        readBackStoredEvents(log2, 100, 1);
        log2.release();
    }

    /**
     * Confirm that we create the log directory automatically if it is possible
     * to do so and fail horribly if we cannot.
     */
    public void testLogDirectoryCreation() throws Exception
    {
        // Ensure log directory does not exist yet.
        File logDir = new File("testLogDirectoryCreation");
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }
        if (logDir.exists())
            throw new Exception(
                    "Unable to delete log directory prior to test: "
                            + logDir.getAbsolutePath());

        // Create and release log.
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializer(ProtobufSerializer.class.getName());
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(1000000);

        log.prepare();
        log.validate();
        log.release();
        assertTrue("Log directory must now exist", logDir.exists());

        // Create a file that will prevent the log directory from being created,
        // then try to instantiate the log.
        File logDir2 = new File("testLogDirectoryCreation2");
        FileWriter fw = new FileWriter(logDir2);
        fw.write("test data");
        fw.close();

        // Create log. Prepare should now fail.
        log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializer(ProtobufSerializer.class.getName());
        log.setLogDir(logDir2.getAbsolutePath());
        log.setLogFileSize(1000000);

        try
        {
            log.prepare();
            throw new Exception("Able to open log on invalid directory: "
                    + logDir2.getAbsolutePath());
        }
        catch (ReplicatorException e)
        {
        }
    }

    /**
     * Confirm that we can write to and read back from the log.
     */
    public void testLogReadback() throws Exception
    {
        // Create the log and write 50 events.
        DiskLog log = openLog("testLogReadback", true);
        this.writeEventsToLog(log, 10000);
        assertEquals("Should have stored 10000 events", 9999, log.getMaxSeqno());
        log.release();

        // Reopen the log and read back.
        DiskLog log2 = openLog("testLogReadback", false);
        log.validate();
        assertEquals("Should have stored 10000 events", 9999, log2
                .getMaxSeqno());
        this.readBackStoredEvents(log2, 0, 10000);
        log2.release();
    }

    /**
     * Confirm that we can write to and read from the log a stream of events
     * that include filtered events.
     */
    public void testLogReadbackWithFiltering() throws Exception
    {
        // Create the log and write groups of events interspersed with
        // filtered events.
        DiskLog log = openLog("testLogReadbackWithFiltering", true);
        long seqno = 0;
        for (int i = 1; i <= 3; i++)
        {
            // Write 7 events.
            this.writeEventsToLog(log, seqno, 7);
            seqno += 7;
            // Filter three events.
            THLEvent e = this.createFilteredTHLEvent(seqno, seqno + 2,
                    (short) i);
            log.store(e, false);
            seqno += 3;
        }
        // 27 is start seqno of last event.
        assertEquals("Should have seqno 27 as last entry", 27, log
                .getMaxSeqno());
        log.validate();
        log.release();

        // Reopen the log and read back events.
        DiskLog log2 = openLog("testLogReadbackWithFiltering", false);
        log.validate();
        assertEquals("Should have seqno 27 as last entry on reopen", 27, log
                .getMaxSeqno());
        seqno = 0;
        for (int i = 1; i <= 24; i++)
        {
            THLEvent e = log2.find(seqno, (short) 0);
            ReplDBMSEvent replEvent = (ReplDBMSEvent) e.getReplEvent();
            if (i % 8 == 0)
            {
                // Every 8th event is a filtered event. It should filter from
                // seqno to seqno + 2.
                assertTrue("Expect a ReplDBMSFilteredEvent",
                        replEvent instanceof ReplDBMSFilteredEvent);
                ReplDBMSFilteredEvent filterEvent = (ReplDBMSFilteredEvent) replEvent;
                assertEquals("Expected start seqno of filtered events", seqno,
                        filterEvent.getSeqno());
                assertEquals("Expected end seqno of filtered events",
                        seqno + 2, filterEvent.getSeqnoEnd());
                seqno += 3;
            }
            else
            {
                // All other events are normal and have the current sequence
                // number.
                assertEquals("Expected seqno of next event", seqno, replEvent
                        .getSeqno());
                seqno++;
            }
        }
        log2.release();
    }

    /**
     * Confirm that we can write to and read back from the log with lots of
     * intervening open and close operations on the file. This checks that we
     * can handle restarts of log clients while preserving log integrity.
     */
    public void testStutteringLogReadback() throws Exception
    {
        // Create the log.
        String name = "testStutteringLogReadback";
        DiskLog log = openLog(name, true);

        // Write a series of events to the log.
        for (int i = 0; i < 100; i++)
        {
            THLEvent e = this.createTHLEvent(i);
            log.store(e, false);

            // Restart the log every 10th call.
            if (i > 0 && i % 10 == 0)
            {
                log.release();
                log = openLog(name, false);
                log.validate();
            }
        }

        assertEquals("Should have stored 100 events", 99, log.getMaxSeqno());

        // Release and reopen the existing log.
        log.release();
        log = null;
        DiskLog log2 = openLog(name, false);
        assertEquals("Should have stored 100 events", 99, log2.getMaxSeqno());

        // Read back events from the log.
        for (int i = 0; i < 100; i++)
        {
            THLEvent e = log2.find(i, (short) 0);
            assertNotNull("Returned event must not be null!", e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(), e
                    .getEventId());

            // Restart the log every 15th call just to be different from
            // previous loop.
            if (i > 0 && i % 15 == 0)
            {
                log2.release();
                log2 = openLog(name, false);
            }
        }

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that trying to find a non-existent sequence number results in a
     * null.
     */
    public void testFindNonExistent() throws Exception
    {
        // Part 1: Look for events on an empty log. This is the trivial
        // case but still important.
        DiskLog log = openLog("testFindNonExistent", true);
        log.release();
        DiskLog logR = openLog("testFindNonExistent", false, 1000000, 1000, 0);

        // Ensure that we don't find anything when the number is less than what
        // is in the log.
        THLEvent e1 = logR.find(-1, (short) 0);
        assertNull("Should not be able to find seqno below log start", e1);

        // Ensure that higher values time out.
        long[] seqno1 = {0, 1, 100};
        for (long seqno : seqno1)
        {
            try
            {
                THLEvent e = logR.find(seqno, (short) 0);
                throw new Exception("Found a non-existent record: "
                        + e.getSeqno());
            }
            catch (LogTimeoutException lte)
            {
            }
        }
        logR.release();

        // Part 2: Look for events on a log with events in it. This is the
        // more realistic case.
        log = openLog("testFindNonExistent", true);
        writeEventsToLog(log, 50);
        log.release();
        logR = openLog("testFindNonExistent", false, 1000000, 1000, 0);
        // Ensure that we don't find anything when the number is less than what
        // is in the log.
        THLEvent e2 = logR.find(-1, (short) 0);
        assertNull("Should not be able to find seqno below log start", e2);

        // Ensure that higher values timeout.
        long[] seqno2 = {50, 51, 100000};
        for (long seqno : seqno2)
        {
            try
            {
                THLEvent e = logR.find(seqno, (short) 0);
                throw new Exception("Found a non-existent record: "
                        + e.getSeqno());
            }
            catch (LogTimeoutException lte)
            {
            }
        }
        logR.release();
    }

    /**
     * Confirm that we can write and read across multiple logs with rotation
     * events.
     */
    public void testMultipleLogs() throws Exception
    {
        // Create the log and write multiple events.
        DiskLog log = openLog("testMultipleLogs", true, 3000);
        writeEventsToLog(log, 200);

        // Assert that we stored the proper number of events in multiple files.
        logger.info("Log file count: " + log.fileCount());
        log.validate();
        log.release();

        // Reopen and read back.
        DiskLog log2 = openLog("testMultipleLogs", false);
        log.validate();
        assertEquals("Should have stored 200 events", 199, log2.getMaxSeqno());

        // Read back expected number of events.
        readBackStoredEvents(log2, 0, 200);

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can delete log files from the bottom to the top.
     */
    public void testDeleteUp() throws Exception
    {
        // Create the log and write multiple events.
        DiskLog log = openLog("testDelete", true, 3000);
        writeEventsToLog(log, 200);

        // Delete from the bottom up.
        for (int i = 0; i < 200; i++)
        {
            log.delete(null, new Long(i));
        }

        // Confirm that we have no log files left.
        log.validate();
        assertEquals("Should have no log files", 0, log.fileCount());
        log.release();

        // Reopen and check.
        DiskLog log2 = openLog("testDelete", false);
        log.validate();

        // Write more events to the log and validate.
        writeEventsToLog(log2, 100);
        readBackStoredEvents(log2, 0, 100);
        log.validate();

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can delete log files from the top back down to the
     * bottom.
     */
    public void testDeleteDown() throws Exception
    {
        // Create the log and write multiple events.
        DiskLog log = openLog("testDeleteDown", true, 3000);
        writeEventsToLog(log, 200);

        // Delete from the top down.
        for (int i = 199; i >= 0; i--)
        {
            log.delete(new Long(i), null);
            if (i > 0)
            {
                long newMaxSeqno = i - 1;
                assertEquals("Expected maximum after truncation", newMaxSeqno,
                        log.getMaxSeqno());
                THLEvent e = log.find(newMaxSeqno, (short) 0);
                assertNotNull("Last event must not be null", e);
            }
        }

        // Confirm that we have no log files left.
        log.validate();
        assertEquals("Should have no log files", 0, log.fileCount());
        log.release();

        // Reopen and check.
        DiskLog log2 = openLog("testDeleteDown", false);
        log.validate();

        // Write more events to the log and validate.
        writeEventsToLog(log2, 100);
        readBackStoredEvents(log2, 0, 100);
        log.validate();

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that logs do not rotate until the last fragment of a transaction.
     */
    public void testFragmentsAndRotation() throws Exception
    {
        // Create log with short file size.
        DiskLog log = openLog("testFragmentsAndRotation", true, 3000);

        // Write fragmented events to the log and confirm that the number
        // of files never changes during a single fragment.
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 100; j++)
            {
                int fileCount = log.fileCount();

                // Write next fragment.
                THLEvent e = createTHLEvent(i, (short) j, false, "test");
                log.store(e, false);
                assertEquals("Seqno should be invariant for fragments", i, log
                        .getMaxSeqno());

                // Log file may not rotate except on the first fragment,
                // which can force a rotation from previous event.
                if (j > 0)
                {
                    assertEquals("Must not rotate log file", fileCount, log
                            .fileCount());
                }
            }

            // Write final fragment.
            THLEvent e = createTHLEvent(i, (short) 100, true, "test");
            log.store(e, true);
            assertEquals("Seqno should be invariant for fragments", i, log
                    .getMaxSeqno());
        }

        // Write a terminating fragment, then confirm the number of fragments
        // is greater than the number of large transactions.
        THLEvent e = createTHLEvent(5, (short) 0, true, "test");
        log.store(e, true);
        assertTrue("Number of fragments >= max seqno", log.fileCount() >= 5);

        // Check and release the log.
        log.validate();
        log.release();

        // Reopen and check size. Ensure we can find the last fragments.
        DiskLog log2 = openLog("testFragmentsAndRotation", false);
        log.validate();
        assertEquals("Should have stored 6 events", 5, log2.getMaxSeqno());
        THLEvent e2 = log2.find(4, (short) 100);
        assertNotNull("Last frag should not be null", e2);
        THLEvent e3 = log2.find(5, (short) 0);
        assertNotNull("Last frag should not be null", e3);
        log2.release();
    }

    /**
     * Confirm that the log drops partial transactions from the end of the log
     * on open. This cleans up the log after a transaction failure and prevents
     * restart failures.
     */
    public void testFragmentCleanup() throws Exception
    {
        // Create log with short file size.
        String logName = "testFragmentCleanup";
        DiskLog log = openLog(logName, true, 3000);

        // Perform a test that writes a few properly terminated transactions
        // into the log and then writes a partial transaction. Reopen the log
        // and confirm that the log is cleaned up.
        long seqno = -1;
        for (int i = 0; i < 20; i++)
        {
            // Write one new sequence number per loop iteration.
            seqno++;

            // Write a properly terminated transaction. We key off the value
            // of the index i to make them gradually increase in size.
            for (int j = 0; j <= i; j++)
            {
                // Write next fragment and confirm no rotation occurs.
                // Terminate and commit the last fragment property.
                THLEvent e = createTHLEvent(seqno, (short) j, (j == i), "test");
                log.store(e, (j == i));
            }

            // Now write an unterminated fragment.
            for (int j = 0; j <= i; j++)
            {
                THLEvent e = createTHLEvent(seqno + 1, (short) j, false, "test");
                log.store(e, true);
            }

            // Close and reopen the log. Confirm that we can find the last full
            // transaction but not the next partial transaction.
            log.release();
            log = openLog(logName, false);

            THLEvent eLastFrag = log.find(seqno, (short) i);
            assertNotNull("Last full xact frag should not be null", eLastFrag);
            assertTrue(
                    "Max seqno in log should be same as last unterminated Xact",
                    log.getMaxSeqno() == eLastFrag.getSeqno());

            // Close and reopen the log to ready for next iteration of writes.
            log.release();
            log = openLog(logName, false);
        }

        log.release();
    }

    /**
     * Confirm that we can read backwards across multiple logs that include log
     * rotation events.
     */
    public void testMultipleLogsBackwardScan() throws Exception
    {
        // Create the log and write multiple events.
        DiskLog log = openLog("testMultipleLogsBackwards", true, 3000);
        writeEventsToLog(log, 50);

        // Assert that we stored the proper number of events in multiple files.
        logger.info("Log file count: " + log.fileCount());
        log.release();

        // Reopen the log and read events back in reverse order.
        DiskLog log2 = openLog("testMultipleLogsBackwards", false);
        for (int i = 49; i >= 0; i--)
        {
            THLEvent e = log2.find(i, (short) 0);
            assertNotNull("Returned event must not be null!", e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(), e
                    .getEventId());
        }

        // Close the log.
        log2.release();
    }

    /**
     * Confirm that we can handle multiple readers accessing the log at
     * different locations while simultaneously writing. We need to make the
     * logs small enough to pick off problems with log rotate events not
     * appearing in time.
     * <p>
     * NOTE: We cannot really test concurrent read and write because reads do
     * not wait until sequence numbers become available.
     */
    public void testConcurrentAccess() throws Exception
    {
        // Create the log and write multiple events.
        DiskLog log = openLog("testConcurrentAccess", true, 1000000);

        // Write a bunch of events to the log.
        writeEventsToLog(log, 10000);

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
        {
            for (Thread thread : tasks.keySet())
            {
                thread.join(30000);

                SimpleLogReader reader = tasks.get(thread);
                if (reader.error != null)
                {
                    throw new Exception(
                            "Reader thread failed with exception after "
                                    + reader.eventsRead + " events",
                            reader.error);
                }
                assertEquals("Checking events read", 10000, reader.eventsRead);
            }
        }

        // Release the log.
        log.release();
    }

    /**
     * Confirm that if the log retention is set we will purge files after the
     * specified interval but that we always retain at least the last two log
     * files.
     */
    public void testLogRetention() throws Exception
    {
        // Create the log with with 5K log files and a 5 second retention.
        DiskLog log = openLog("testLogRetention", true, 3000, 10000, 5000);
        writeEventsToLog(log, 200);

        // Collect the log file count and ensure it is greater than two.
        int fileCount = log.fileCount();
        assertTrue("More than two logs generated", fileCount > 2);

        // Wait for the retention to expire.
        Thread.sleep(10000);

        // Write enough events to force log rotation by computing how many
        // events on average go into a single log.
        int logEvents = (200 / fileCount) * 2;
        writeEventsToLog(log, 200, logEvents);

        // Give the deletion thread time to do its work.
        Thread.sleep(3000);

        // We should now have 2 logs because the old logs will age out.
        int fileCount2 = log.fileCount();
        assertEquals("Aging out should result in 2 logs", 2, fileCount2);

        // All done!
        log.release();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private void prepareLogDir(File logDir)
    {
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
    }

    // Open a new or existing log.
    private DiskLog openLog(String logDirName, boolean create, int fileSize,
            int timeoutMillis, int logFileRetainMillis)
            throws ReplicatorException, InterruptedException
    {
        // Create the log directory if this is a new log.
        File logDir = new File(logDirName);
        if (create)
        {
            prepareLogDir(logDir);
        }
        DiskLog log = new DiskLog();
        log.setDoChecksum(true);
        log.setEventSerializer(serializer.getName());
        log.setLogDir(logDir.getAbsolutePath());
        log.setLogFileSize(fileSize);
        log.setTimeoutMillis(timeoutMillis);
        log.setLogFileRetainMillis(logFileRetainMillis);
        log.prepare();

        return log;
    }

    // Default open to create log with 10 second read timeout.
    private DiskLog openLog(String logDirName, boolean create, int fileSize)
            throws ReplicatorException, InterruptedException
    {
        return openLog(logDirName, create, fileSize, 10000, 0);
    }

    // Open new or existing with default size of 1M bytes.
    private DiskLog openLog(String logDirName, boolean create)
            throws ReplicatorException, InterruptedException
    {
        return openLog(logDirName, create, 1000000);
    }

    // Write a prescribed number of events to the log starting at zero.
    private void writeEventsToLog(DiskLog log, int howMany)
            throws THLException, InterruptedException
    {
        writeEventsToLog(log, 0, howMany);
    }

    // Write a prescribed number of events to the log starting at a
    // specified sequence number.
    private void writeEventsToLog(DiskLog log, long seqno, int howMany)
            throws THLException, InterruptedException
    {
        // Write a series of events to the log.
        for (int i = 0; i < howMany; i++)
        {
            THLEvent e = this.createTHLEvent(seqno++);
            log.store(e, false);
            if (i > 0 && i % 1000 == 0)
                logger.info("Writing events to disk: seqno=" + i);
        }
        assertEquals("Should have stored requested events", (seqno - 1), log
                .getMaxSeqno());
    }

    // Read back a prescribed number of events.
    private void readBackStoredEvents(DiskLog log, long fromSeqno, long count)
            throws THLException, InterruptedException
    {
        // Read back events from the log.
        for (long i = fromSeqno; i < fromSeqno + count; i++)
        {
            THLEvent e = log.find(i, (short) 0);
            assertNotNull("Returned event must not be null!", e);
            assertEquals("Test expected seqno", i, e.getSeqno());
            assertEquals("Test expected fragno", (short) 0, e.getFragno());
            assertEquals("Test expected eventId", new Long(i).toString(), e
                    .getEventId());
            if (i > 0 && i % 1000 == 0)
                logger.info("Reading events from disk: seqno=" + i);
        }
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

    // Create a dummy THL event that contains a filtered event.
    private THLEvent createFilteredTHLEvent(long fromSeqno, long toSeqno,
            short toFragno)
    {
        String eventId = new Long(toSeqno).toString();
        ReplDBMSFilteredEvent filterEvent = new ReplDBMSFilteredEvent(eventId,
                fromSeqno, toSeqno, toFragno);
        return new THLEvent(eventId, filterEvent);
    }

    private THLEvent createTHLEvent(long seqno)
    {
        return createTHLEvent(seqno, (short) 0, true, "test");
    }
}

// Local class to read records.
class SimpleLogReader implements Runnable
{
    private static Logger logger = Logger.getLogger(SimpleLogReader.class);
    DiskLog               log;
    long                  startSeqno;
    int                   howMany;
    int                   eventsRead;
    Throwable             error;

    /** Store file instance. */
    SimpleLogReader(DiskLog log, long startSeqno, int howMany)
    {
        this.log = log;
        this.startSeqno = startSeqno;
        this.howMany = howMany;
    }

    /** Read all records from file. */
    public void run()
    {
        for (long seqno = startSeqno; seqno < startSeqno + howMany; seqno++)
        {
            try
            {
                THLEvent e = log.find(seqno, (short) 0);
                if (e == null)
                    throw new Exception("Event is null: seqno=" + seqno);
                if (seqno != e.getSeqno())
                {
                    throw new Exception(
                            "Sequence numbers do not match: expected=" + seqno
                                    + " actual=" + e.getSeqno());
                }
                eventsRead++;

                if (eventsRead > 0 && eventsRead % 1000 == 0)
                {
                    logger.info("Reading events: threadId="
                            + Thread.currentThread().getId() + " events="
                            + eventsRead);
                }
            }
            catch (Throwable t)
            {
                error = t;
                break;
            }
        }
    }
}