/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.thl;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.management.MockEventDispatcher;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;
import com.continuent.tungsten.replicator.storage.Store;

/**
 * Implements a test of THL. This test implements a practical test of the
 * pipeline architecture under various transaction use cases, which are
 * documented below.
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class TestTHL2 extends TestCase
{
    private static Logger logger = Logger.getLogger(TestTHL2.class);

    /*
     * Verify that we can start a THL as a store in a pipeline.
     */
    public void testBasicService() throws Exception
    {
        logger.info("##### testBasicService #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTwoStageProps(
                "testBasicServices", 1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 server events", 9, lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0, thl.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9, thl.getMaxStoredSeqno());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that two THLs may be chained together using separate pipelines and
     * that following replication they contain the same number of events.
     */
    public void testTHL2Chaining() throws Exception
    {
        logger.info("##### testTHL2Chaining #####");

        // Prepare the log directories.
        prepareLogDir("testTHL2Chaining1");
        prepareLogDir("testTHL2Chaining2");

        // Generate server pipeline from dummy extractor to THL.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, "testTHL2Chaining1");
        builder.addPipeline("master", "extract-s", "thl");
        builder.addStage("extract-s", "dummy", "thl-apply", null);

        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testTHL2Chaining1");
        builder.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2112/");
        TungstenProperties serverConf = builder.getConfig();

        // Generate server pipeline from remote extractor to THL to dummy
        // applier.
        PipelineConfigBuilder builder2 = new PipelineConfigBuilder();
        builder2.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder2.setRole("master");
        builder2.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testTHL2Chaining2");
        builder2.addPipeline("master", "extract-c,apply-c", "thl");
        builder2.addStage("extract-c", "thl-remote-extractor", "thl-apply",
                null);
        builder2.addStage("apply-c", "thl-extract", "dummy", null);

        builder2.addComponent("extractor", "thl-remote-extractor",
                RemoteTHLExtractor.class);
        builder2.addProperty("extractor", "thl-remote-extractor", "connectUri",
                "thl://localhost:2112/");
        builder2.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder2.addProperty("applier", "thl-apply", "storeName", "thl");

        builder2.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder2.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder2.addComponent("applier", "dummy", DummyApplier.class);

        builder2.addComponent("store", "thl", THL.class);
        builder2.addProperty("store", "thl", "logDir", "testTHL2Chaining2");
        builder2.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2113/");

        TungstenProperties clientConf = builder2.getConfig();

        // Configure and get pipelines.
        ReplicatorRuntime serverRuntime = new ReplicatorRuntime(serverConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        serverRuntime.configure();
        serverRuntime.prepare();
        Pipeline serverPipeline = serverRuntime.getPipeline();

        ReplicatorRuntime clientRuntime = new ReplicatorRuntime(clientConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        clientRuntime.configure();
        clientRuntime.prepare();
        Pipeline clientPipeline = clientRuntime.getPipeline();

        // Start both pipelines.
        serverPipeline.start(new MockEventDispatcher());
        clientPipeline.start(new MockEventDispatcher());

        // Wait for both pipelines to finish.
        Future<ReplDBMSHeader> waitServer = serverPipeline
                .watchForAppliedSequenceNumber(9);
        Future<ReplDBMSHeader> waitClient = clientPipeline
                .watchForAppliedSequenceNumber(9);

        logger.info("Waiting for server pipeline to clear");
        ReplDBMSHeader lastMasterEvent = waitServer.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 server events", 9, lastMasterEvent.getSeqno());

        logger.info("Waiting for client pipeline to clear");
        ReplDBMSHeader lastClientEvent = waitClient.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 client events", 9, lastClientEvent.getSeqno());

        // Ensure each THL contains expected number of events.
        Store serverThl = serverPipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0,
                serverThl.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9,
                serverThl.getMaxStoredSeqno());

        Store thlClient = clientPipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0,
                thlClient.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9,
                thlClient.getMaxStoredSeqno());

        // Shut down both pipelines.
        clientPipeline.shutdown(true);
        serverPipeline.shutdown(true);
        clientRuntime.release();
        serverRuntime.release();
    }

    /**
     * Verify that multiple pipelines work slave pipeline extracts from the
     * master pipeline.
     */
    public void testInstanceConnections() throws Exception
    {
        logger.info("##### testInstanceConnections #####");

        // Prepare log directory.
        prepareLogDir("testInstanceConnections");

        // Generate server pipeline from dummy extractor to THL.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "extract", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testInstanceConnections");
        TungstenProperties serverConf = builder.getConfig();

        // Generate slave pipeline from remote THL extractor to dummy applier.
        PipelineConfigBuilder builder2 = new PipelineConfigBuilder();
        builder2.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder2.setRole("slave");
        builder2.addPipeline("slave", "extract", null);
        builder2.addStage("extract", "thl-remote-extractor", "dummy", null);
        builder2.addComponent("extractor", "thl-remote-extractor",
                RemoteTHLExtractor.class);
        builder2.addComponent("applier", "dummy", DummyApplier.class);
        TungstenProperties clientConf = builder2.getConfig();

        // Configure and get pipelines.
        ReplicatorRuntime serverRuntime = new ReplicatorRuntime(serverConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        serverRuntime.configure();
        serverRuntime.prepare();
        Pipeline serverPipeline = serverRuntime.getPipeline();

        ReplicatorRuntime clientRuntime = new ReplicatorRuntime(clientConf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        clientRuntime.configure();
        clientRuntime.prepare();
        Pipeline clientPipeline = clientRuntime.getPipeline();

        // Start both pipelines.
        serverPipeline.start(new MockEventDispatcher());
        clientPipeline.start(new MockEventDispatcher());

        // Wait for both pipelines to finish.
        Future<ReplDBMSHeader> waitServer = serverPipeline
                .watchForAppliedSequenceNumber(9);
        Future<ReplDBMSHeader> waitClient = clientPipeline
                .watchForAppliedSequenceNumber(9);

        logger.info("Waiting for server pipeline to clear");
        ReplDBMSHeader lastMasterEvent = waitServer.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 server events", 9, lastMasterEvent.getSeqno());

        logger.info("Waiting for client pipeline to clear");
        ReplDBMSHeader lastClientEvent = waitClient.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 client events", 9, lastClientEvent.getSeqno());

        // Ensure THL contains expected number of events.
        Store thl = serverPipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0, thl.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9, thl.getMaxStoredSeqno());

        // Shut down both pipelines.
        clientPipeline.shutdown(true);
        serverPipeline.shutdown(true);
        clientRuntime.release();
        serverRuntime.release();
    }

    /**
     * Verify that if we store events in a THL, shutdown, and then restart a new
     * pipeline referring to the same in-memory storage, the starting sequence
     * number is correctly propagated back to the extractor so that new events
     * begin at the next sequence number.
     */
    public void testSeqnoPropagation() throws Exception
    {
        logger.info("##### testSeqnoPropagation #####");

        this.prepareLogDir("testSeqnoPropagation");

        // Generate config.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testSeqnoPropagation");
        builder.addPipeline("master", "extract-s", "thl");
        builder.addStage("extract-s", "dummy", "thl-apply", null);

        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testSeqnoPropagation");
        builder.addProperty("store", "thl", "storageListenerUri",
                "thl://localhost:2112/");
        TungstenProperties conf = builder.getConfig();

        // Run pipeline through the first time.
        ReplicatorRuntime runtime1 = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime1.configure();
        runtime1.prepare();
        Pipeline pipeline1 = runtime1.getPipeline();
        pipeline1.start(new MockEventDispatcher());

        // Wait for pipeline to finish.
        Future<ReplDBMSHeader> wait1 = pipeline1
                .watchForAppliedSequenceNumber(9);
        logger.info("Waiting for pipeline #1 to clear");
        ReplDBMSHeader lastEvent1 = wait1.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 events", 9, lastEvent1.getSeqno());

        // Shut down first pipeline.
        pipeline1.shutdown(true);
        runtime1.release();

        // Run pipeline through the second time.
        ReplicatorRuntime runtime2 = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime2.configure();
        runtime2.prepare();
        Pipeline pipeline2 = runtime2.getPipeline();
        pipeline2.start(new MockEventDispatcher());

        // Wait for pipeline to finish. It should get to event #19.
        Future<ReplDBMSHeader> wait2 = pipeline2
                .watchForAppliedSequenceNumber(19);
        logger.info("Waiting for pipeline #2 to clear");
        ReplDBMSHeader lastEvent2 = wait2.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 20 events", 19, lastEvent2.getSeqno());

        // Ensure THL contains expected number of events.  We must sleep 
        // very briefly to allow the THL to commit. 
        Thread.sleep(50);
        Store thl = pipeline2.getStore("thl");
        assertEquals("Expected 0 as first event", 0, thl.getMinStoredSeqno());
        assertEquals("Expected 19 as last event", 19, thl.getMaxStoredSeqno());

        // Shut down second pipeline.
        pipeline2.shutdown(true);
        runtime2.release();
    }

    /**
     * Verify that fragmented events are correctly replicated and stored.
     */
    public void testFragmentedEvents() throws Exception
    {
        logger.info("##### testFragmentedEvents #####");

        // Prepare log directory.
        this.prepareLogDir("testFragmentedEvents");

        // Create configuration; ask dummy extractor to generate 3 fragments
        // per transaction.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testFragmentedEvents");
        builder.addPipeline("master", "extract, apply", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags",
                new Integer(3).toString());
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", "testFragmentedEvents");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);
        builder.addProperty("applier", "dummy", "storeAppliedEvents", "true");

        TungstenProperties conf = builder.getConfig();
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());

        // Configure and start pipeline
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
        assertEquals("Expected 10 server events", 9, lastEvent.getSeqno());

        Store thl = pipeline.getStore("thl");
        assertEquals("Expected 0 as first event", 0, thl.getMinStoredSeqno());
        assertEquals("Expected 9 as last event", 9, thl.getMaxStoredSeqno());

        // Confirm we have 10x2 statements.
        ApplierWrapper wrapper = (ApplierWrapper) pipeline.getStage("apply")
                .getApplier0();
        DummyApplier applier = (DummyApplier) wrapper.getApplier();
        ArrayList<StatementData> sql = ((DummyApplier) applier).getTrx();
        assertEquals("Expected 10x2 statements", 60, sql.size());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /*
     * Verify that the THLExtractor extract() method waits for events to arrive
     * in the THL and then supplies them to the waiting THL. This simulates the
     * case where an applier stage has applied everything from the stage and is
     * now waiting for new events to arrive.
     */
    public void testTHLExtractWaiting() throws Exception
    {
        logger.info("##### testTHLExtractWaiting #####");

        // Set up a pipeline with a queue at the beginning. We will feed
        // transactions into the queue.
        TungstenProperties conf = this
                .generateQueueFedPipelineProps("testTHLExtractWaiting");
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Fetch out the queue store so we can write events thereunto.
        InMemoryQueueStore queue = (InMemoryQueueStore) pipeline
                .getStore("queue");

        // Feed events into the pipeline and confirm they reach the other side.
        for (int i = 0; i < 10; i++)
        {
            assertFalse("Pipeline must be OK", pipeline.isShutdown());

            // Create and insert an event.
            ReplDBMSEvent e = createEvent(i);
            queue.put(e);

            // Now wait for it.
            Future<ReplDBMSHeader> wait = pipeline
                    .watchForAppliedSequenceNumber(i);
            ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
            assertEquals("Expected event we put in", i, lastEvent.getSeqno());
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();

        // Restart the pipeline and do the same test again, starting with event
        // 10.
        runtime = new ReplicatorRuntime(conf, new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        pipeline = runtime.getPipeline();
        pipeline.start(new MockEventDispatcher());

        // Feed events into the pipeline and confirm they reach the other side.
        // We have a sleep to simulate not getting events into the THL for a
        // while.
        Thread.sleep(1000);
        queue = (InMemoryQueueStore) pipeline.getStore("queue");
        for (int i = 10; i < 20; i++)
        {
            assertFalse("Pipeline must be OK", pipeline.isShutdown());

            // Create and insert an event.
            ReplDBMSEvent e = createEvent(i);
            queue.put(e);

            // Now wait for it.
            Future<ReplDBMSHeader> wait = pipeline
                    .watchForAppliedSequenceNumber(i);
            ReplDBMSHeader lastEvent = wait.get(5, TimeUnit.SECONDS);
            assertEquals("Expected event we put in", i, lastEvent.getSeqno());
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    // Generate configuration properties for a double stage-pipline
    // going through THL.
    public TungstenProperties generateTwoStageProps(String schemaName,
            int nFrags) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract, apply", "thl");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags",
                new Integer(nFrags).toString());
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate configuration properties for a double stage-pipeline
    // going through THL. This pipeline uses a queue as the initial head
    // of the queue.
    public TungstenProperties generateQueueFedPipelineProps(String schemaName)
            throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract, apply", "queue,thl");
        builder.addStage("extract", "queue", "thl-apply", null);
        builder.addStage("apply", "thl-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "queue", InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "queue", "storeName", "queue");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Apply stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Create an empty log directory or if the directory exists remove
    // any files within it.
    private File prepareLogDir(String logDirName)
    {
        File logDir = new File(logDirName);
        // Delete old log if present.
        if (logDir.exists())
        {
            for (File f : logDir.listFiles())
            {
                f.delete();
            }
            logDir.delete();
        }

        // Create new log directory.
        logDir.mkdirs();
        return logDir;
    }

    // Returns a well-formed event with a default shard ID.
    private ReplDBMSEvent createEvent(long seqno)
    {
        return createEvent(seqno, ReplOptionParams.SHARD_ID_UNKNOWN);
    }

    // Returns a well-formed ReplDBMSEvent with a specified shard ID.
    private ReplDBMSEvent createEvent(long seqno, String shardId)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, dbmsEvent);
        replDbmsEvent.getDBMSEvent().addMetadataOption(
                ReplOptionParams.SHARD_ID, shardId);
        return replDbmsEvent;
    }
}