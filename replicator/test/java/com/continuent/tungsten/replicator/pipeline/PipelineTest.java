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
 */

package com.continuent.tungsten.replicator.pipeline;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;

/**
 * This class implements a test of the Pipeline class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineTest extends TestCase
{
    private static Logger  logger = Logger.getLogger(PipelineTest.class);
    private PipelineHelper helper = new PipelineHelper();

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
     * Verify that we can build and immediately release a pipeline without
     * starting.
     */
    public void testSimplePipeline() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();

        Pipeline pipeline = runtime.getPipeline();
        pipeline.release(runtime);
    }

    /**
     * Verify that we can build, start, stop, and release a pipeline without
     * failures.
     */
    public void testStartStopPipeline() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that the pipeline tracks processed sequence numbers.
     */
    public void testSeqnoTracking() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 9 after 10 Xacts", 9, seqno);
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can skip transactions on start up using method
     * Stage.applySkipCount().
     */
    public void testSeqnoSkip() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Store events in the applier.
        DummyApplier dummy = (DummyApplier) ((ApplierWrapper) pipeline
                .getTailApplier()).getApplier();
        dummy.setStoreAppliedEvents(true);

        // Set the skip count.
        Stage stage = pipeline.getStages().get(0);
        stage.applySkipCount = 5;
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Ensure that we have gotten the right count of extract transactions.
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 9 after 10 Xacts", 9, seqno);

        // Ensure we have only a few applied transactions.
        long stmtCount = dummy.getTrx().size();
        assertEquals(
                "Expect statement count to be 10 after skipping 5 of 10 transactions",
                10, stmtCount);

        // Close up and go home.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * sequence ID.
     */
    public void testRunToSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Shut down pipeline at seqno 5.
        Pipeline pipeline = runtime.getPipeline();
        Future<Pipeline> future = pipeline.shutdownAfterSequenceNumber(5);
        startAndAssertEventsApplied(pipeline, future);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * native event ID. (Identical to previous case, but using the event ID.)
     */
    public void testRunToEventId() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Shut down pipeline at seqno 5.
        Pipeline pipeline = runtime.getPipeline();
        Future<Pipeline> future = pipeline.shutdownAfterEventId("5");
        startAndAssertEventsApplied(pipeline, future);
        pipeline.release(runtime);
    }

    // Start pipeline and assert number of events completed.
    private void startAndAssertEventsApplied(Pipeline pipeline,
            Future<Pipeline> wait) throws Exception
    {
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Ensure future completes.
        Pipeline p = wait.get(5, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);

        // Ensure that we have gotten the right count of extract transactions.
        long seqno = pipeline.getLastExtractedSeqno();
        assertEquals("Expect seqno to be 5 after shutdown", 5, seqno);

        // Ensure we have only a few extracted transactions.
        RawApplier ra = ((ApplierWrapper) pipeline.getTailApplier())
                .getApplier();
        long eventCount = ((DummyApplier) ra).getEventCount();
        assertEquals("Expect count to be 6 after exiting @ seqno=5", 6,
                eventCount);

        // Close up and go home.
        pipeline.shutdown(false);
    }

    /**
     * Verify that we can set up the pipeline to go offline at a particular
     * sequence ID.
     */
    public void testPipelineShutdownAfterEvent() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();

        // Start pipeline and let it run.
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());
        Thread.sleep(1000);

        // Shut down at an event we have already reached and assert that
        // it is shut down.
        Future<Pipeline> future = pipeline.shutdownAfterEventId("9");
        Pipeline p = future.get(60, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);
        assertTrue("Pipeline should have shut down", pipeline.isShutdown());

        // Close up and go home.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can shut down a running pipeline at a particular sequence
     * number. This test is approximate, since it depends on thread timing.
     * Outcomes are unambiguous.
     */
    public void testActivePipelineShutdownAtSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Set dummy extractor to do 1M transactions, but don't store anything.
        Stage stage0 = pipeline.getStages().get(0);
        stage0.setLoggingInterval(100000);
        ExtractorWrapper ew = (ExtractorWrapper) stage0.getExtractor0();
        DummyExtractor de = (DummyExtractor) ew.getExtractor();
        de.setNTrx(5000000);
        ApplierWrapper aw = (ApplierWrapper) stage0.getApplier0();
        DummyApplier da = (DummyApplier) aw.getApplier();
        da.setStoreAppliedEvents(false);

        // Tell the pipeline to stop after 1m events.
        Future<Pipeline> future = pipeline.shutdownAfterSequenceNumber(999999);
        Pipeline p = future.get(90, TimeUnit.SECONDS);
        assertEquals("Future should return pipeline", pipeline, p);

        // Ensure that we got not less than 1m events.
        long seqno = pipeline.getLastExtractedSeqno();
        assertTrue("Expect seqno to be >= 1m", seqno >= 999999);
        if (seqno == 999999)
            logger.info("Got exactly 1m events!");

        // Ensure we have at least 1m events stored.
        RawApplier ra = ((ApplierWrapper) pipeline.getTailApplier())
                .getApplier();
        long eventCount = ((DummyApplier) ra).getEventCount();
        assertTrue("Expect count >= 1000000", eventCount >= 1000000);

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that waiting for an existing sequence number returns immediately
     * whereas waiting for a higher number times out.
     */
    public void testWaitSeqno() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Test for successfully applied and extracted sequence numbers.
        Future<ReplDBMSHeader> future = pipeline
                .watchForAppliedSequenceNumber(9);
        ReplDBMSHeader matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Applied sequence number matches",
                matchingEvent.getSeqno() >= 9);
        assertTrue("Applied seqnence number not higher",
                matchingEvent.getSeqno() < 10);

        future = pipeline.watchForExtractedSequenceNumber(9);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Extracted sequence number matches",
                matchingEvent.getSeqno() >= 9);
        assertTrue("Extracted seqnence number not higher",
                matchingEvent.getSeqno() < 10);

        // Check for successfully applied and extracted event IDs.
        String eventId = matchingEvent.getEventId();
        future = pipeline.watchForExtractedEventId(eventId);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Extracted event ID matches",
                eventId.equals(matchingEvent.getEventId()));

        future = pipeline.watchForAppliedEventId(eventId);
        matchingEvent = future.get(3, TimeUnit.SECONDS);
        assertTrue("Applied event ID matches",
                eventId.equals(matchingEvent.getEventId()));

        // Test for higher numbers, which should time out.
        future = pipeline.watchForExtractedSequenceNumber(99);
        try
        {
            matchingEvent = future.get(1, TimeUnit.SECONDS);
            throw new Exception(
                    "Wait for extracted event did not time out! seqno="
                            + matchingEvent.getSeqno());
        }
        catch (TimeoutException e)
        {
        }
        future = pipeline.watchForAppliedSequenceNumber(99);
        try
        {
            matchingEvent = future.get(1, TimeUnit.SECONDS);
            throw new Exception(
                    "Wait for applied event did not time out! seqno="
                            + matchingEvent.getSeqno());
        }
        catch (TimeoutException e)
        {
        }

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can handle a two stage pipeline with an intervening store.
     */
    public void testTwoStages() throws Exception
    {
        // Create config with pipeline that has no fragmentation.
        TungstenProperties config = helper.createRuntimeWithStore(0);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Test for successfully applied and extracted sequence numbers.
        Future<ReplDBMSHeader> future = pipeline
                .watchForAppliedSequenceNumber(9);
        ReplDBMSHeader matchingEvent = future.get(2, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", 9,
                matchingEvent.getSeqno());

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that we can handle fragmented events where each event consists of
     * N fragments.
     */
    public void testFragmentHandling() throws Exception
    {
        // Create configuration; ask dummy extractor to generate 3 fragments
        // per transaction.
        TungstenProperties conf = helper.createRuntimeWithStore(3);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Configure pipeline. Set dummy applier to store events so we an fetch
        // them later.
        runtime.configure();
        Pipeline pipeline = runtime.getPipeline();
        ApplierWrapper wrapper = (ApplierWrapper) pipeline.getStage("apply")
                .getApplier0();
        DummyApplier applier = (DummyApplier) wrapper.getApplier();
        applier.setStoreAppliedEvents(true);

        // Prepare and start the pipeline.
        runtime.prepare();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(10, TimeUnit.SECONDS);
        assertEquals("Expected 10 sequence numbers", 9, lastEvent.getSeqno());

        // Confirm we have 30x2 statements, i.e., two statements for each
        // sequence number.
        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        assertEquals("Expected 30x2 statements", 60, sql.size());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that we can handle 10M events without problems.
     */
    public void testManyEvents() throws Exception
    {
        int maxEvents = 10000000;
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Set dummy extractor to do 1M transactions, but don't store anything.
        Stage stage0 = pipeline.getStages().get(0);
        stage0.setLoggingInterval(1000000);
        ExtractorWrapper ew = (ExtractorWrapper) stage0.getExtractor0();
        DummyExtractor de = (DummyExtractor) ew.getExtractor();
        de.setNTrx(maxEvents);
        ApplierWrapper aw = (ApplierWrapper) stage0.getApplier0();
        DummyApplier da = (DummyApplier) aw.getApplier();
        da.setStoreAppliedEvents(false);

        // Test for successfully applied and extracted sequence numbers.
        Future<ReplDBMSHeader> future = pipeline
                .watchForAppliedSequenceNumber(maxEvents - 1);
        ReplDBMSHeader matchingEvent = future.get(600, TimeUnit.SECONDS);
        assertEquals("Applied sequence number matches", maxEvents - 1,
                matchingEvent.getSeqno());

        // Shut it down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }
}