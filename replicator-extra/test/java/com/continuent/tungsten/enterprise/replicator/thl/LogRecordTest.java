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

import java.sql.Timestamp;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.enterprise.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.enterprise.replicator.thl.serializer.Serializer;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.thl.THLEvent;

/**
 * Tests ability to write and read back more or less realistic log records
 * containing replication and log rotation events.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogRecordTest extends TestCase
{
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
     * Confirm that we can write and then read back a replication event.
     */
    public void testReplicationEvents() throws Exception
    {
        // Open log file.
        Serializer serializer = new ProtobufSerializer();
        LogFile tfrw = LogHelper.createLogFile("testReplicationEvents.dat", 3);

        // Add a THL event.
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ReplDBMSEvent replEvent = new ReplDBMSEvent(31, (short) 2, true,
                "unittest", 1, now, new DBMSEvent());
        THLEvent inputEvent = new THLEvent("dummy", replEvent);

        LogEventReplWriter writer = new LogEventReplWriter(inputEvent,
                serializer, true);
        LogRecord logRec = writer.write();
        tfrw.writeRecord(logRec, 10000);
        tfrw.release();

        // Read the same THL event back.
        LogFile tfro = LogHelper
                .openExistingFileForRead("testReplicationEvents.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        LogEventReplReader reader = new LogEventReplReader(logRec2, serializer,
                true);
        THLEvent outputEvent = reader.deserializeEvent();
        reader.done();

        // Check header fields.
        assertEquals("Checking recordType", LogRecord.EVENT_REPL, reader
                .getRecordType());
        assertEquals("Checking setno", 31, reader.getSeqno());
        assertEquals("Checking fragment", 2, reader.getFragno());
        assertEquals("Checking last frag", true, reader.isLastFrag());
        assertEquals("Checking epoch", 1, reader.getEpochNumber());
        assertEquals("Checking sourceId", "unittest", reader.getSourceId());
        assertEquals("Checking eventId", "dummy", reader.getEventId());
        assertEquals("Checking shardId", "#DEFAULT", reader.getShardId());
        assertEquals("Checking source tstamp", inputEvent.getSourceTstamp(),
                new Timestamp(reader.getSourceTStamp()));

        // Check THLEvent.
        assertNotNull("Event deserialized", outputEvent);
        assertEquals("Event seqno", inputEvent.getSeqno(), outputEvent
                .getSeqno());

        tfro.release();
    }

    /**
     * Confirm that we can write and then read back a log rotation event.
     */
    public void testRotationEvents() throws Exception
    {
        // Open log file.
        LogFile tfrw = LogHelper.createLogFile("testRotationEvents.dat", 3);

        // Add a log rotation event.
        LogEventRotateWriter writer = new LogEventRotateWriter(45, true);
        LogRecord logRec = writer.write();
        tfrw.writeRecord(logRec, 10000);
        tfrw.release();

        // Read the rotation event back. 
        LogFile tfro = LogHelper.openExistingFileForRead("testRotationEvents.dat");
        LogRecord logRec2 = tfro.readRecord(0);
        LogEventRotateReader reader = new LogEventRotateReader(logRec2,
                true);

        // Check header fields.
        assertEquals("Checking recordType", LogRecord.EVENT_ROTATE, reader
                .getRecordType());
        assertEquals("Checking index", 45, reader.getIndex());

        tfro.release();
    }
}

