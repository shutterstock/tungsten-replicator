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
import com.continuent.tungsten.replicator.EventDispatcher;
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
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueue;
import com.continuent.tungsten.replicator.storage.InMemoryMultiQueueApplier;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.storage.parallel.HashPartitioner;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * Implements a test of parallel THL operations. Parallel THL operation requires
 * a pipeline THL coupled with a THLParallelQueue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THLParallelQueueTest extends TestCase
{
    private static Logger logger = Logger.getLogger(THLParallelQueueTest.class);

    /*
     * Verify that we can start and stop a pipeline containing a THL with a
     * THLParallelQueue.
     */
    public void testPipelineStartStop() throws Exception
    {
        logger.info("##### testPipelineStartStop #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testPipelineStartStop", 1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /*
     * Verify that a pipeline with a single channel successfully transmits
     * events from end to end.
     */
    public void testSingleChannel() throws Exception
    {
        logger.info("##### testSingleChannel #####");

        // Set up and start pipelines.
        TungstenProperties conf = this.generateTHLParallelQueueProps(
                "testSingleChannel", 1);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

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
     * Verify that on-disk queues increment the serialization count each time a
     * serialized event is processed.
     */
    public void testSerialization() throws Exception
    {
        logger.info("##### testSerialization #####");

        // Set up and prepare pipeline. We set the channel count to
        // 1 as we just want to confirm that serialization counts are
        // increasing.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testSerialization", 1, 50, 100);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        THLParallelQueue tpq = (THLParallelQueue) pipeline
                .getStore("thl-queue");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write and read back 99 events where every third event is #UNKNOWN,
        // hence should be serialized by the HashSerializer class.
        int serialized = 0;
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 99; i++)
        {
            // Get the serialization count from the store.
            int serializationCount = getSerializationCount(tpq);

            // Insert and read back an event from the end of the pipeline.
            String shardId = (i % 3 == 0 ? "#UNKNOWN" : "db0");
            ReplDBMSEvent rde = this.createEvent(i, shardId);
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
            conn.commit();
            ReplDBMSEvent rde2 = mq.get(0);

            // Ensure that we got the event back and that the serialization
            // count incremented by one *only* for #UNKNOWN events.
            assertEquals("Read back same event", rde.getSeqno(),
                    rde2.getSeqno());
            int serializationCount2 = getSerializationCount(tpq);
            if ("#UNKNOWN".equals(rde.getShardId()))
            {
                serialized++;
                assertEquals("Expect serialization to increment",
                        serializationCount + 1, serializationCount2);
            }
            else
            {
                assertEquals("Expect serialization to remain the same",
                        serializationCount, serializationCount2);
            }
        }

        // Ensure we serialized 33 events in total.
        assertEquals("Serialization total", 33, serialized);

        // Close down pipeline.
        thl.disconnect(conn);
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that a parallel THL queue with more than one partition assigns
     * each event to the correct channel. This test uses 3 channels with
     * partitioning on shard name. We write and read directly to/from the linked
     * THL and THLParallelQueue to confirm behavior.
     */
    public void testMultiChannelBasic() throws Exception
    {
        logger.info("##### testMultiChannelBasic #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testMultiChannelBasic", 3, 50, 100);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write events to the THL with three different shard IDs.
        LogConnection conn = thl.connect(false);
        for (int i = 0; i < 90; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db" + (i % 3));
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Confirm that each parallel queue on the other side gets 30 events and
        // that said events are partially ordered within each queue.
        for (int q = 0; q < 3; q++)
        {
            long seqno = -1;
            String shardId = "db" + q;
            for (int i = 0; i < 30; i++)
            {
                ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(q);
                assertTrue("Seqno increases due to partial order",
                        rde2.getSeqno() > seqno);
                assertEquals("Shard ID matches queue", shardId,
                        rde2.getShardId());
            }
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that a parallel THL queue with more than one partition allows
     * reads from one partition even when another partition is filled to
     * capacity. This proves that the parallel THL queue can handle a very large
     * gap between the positions of different partitions.
     */
    public void testLaggingChannels() throws Exception
    {
        logger.info("##### testLaggingChannels #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testLaggingChannels", 3, 50, 100);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events on an initial shard ID. This should be
        // far greater than the maxSize parameter of the queue.
        LogConnection conn = thl.connect(false);
        logger.info("Writing db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db0");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();

        // Write 100 events on a second shard ID.
        logger.info("Writing db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde = this.createEvent(i, "db1");
            THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
            conn.store(thlEvent, false);
        }
        conn.commit();
        thl.disconnect(conn);

        // Read the 100 events on queue 1 first and confirm seqno as well as
        // shard ID.
        logger.info("Reading db1 events");
        for (int i = 100000; i < 100100; i++)
        {
            ReplDBMSEvent rde2 = (ReplDBMSEvent) mq.get(1);
            assertEquals("Seqno matches expected for this queue", i,
                    rde2.getSeqno());
            assertEquals("Shard ID matches queue", "db1", rde2.getShardId());
        }

        // Now read the remaining events on queue 0.
        logger.info("Reading db0 events");
        for (int i = 0; i < 100000; i++)
        {
            ReplDBMSEvent rde3 = (ReplDBMSEvent) mq.get(0);
            assertEquals("Seqno matches expected for this queue", i,
                    rde3.getSeqno());
            assertEquals("Shard ID matches queue", "db0", rde3.getShardId());
            if (i % 10000 == 0)
                logger.info("Current seqno: " + rde3.getSeqno());
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    /**
     * Verify that the parallel queue correctly transfers data in pipeline where
     * only a few of many channels are actually used.
     */
    public void testMultiChannelLag() throws Exception
    {
        logger.info("##### testMultiChannelLag #####");

        // Set up and prepare pipeline.
        TungstenProperties conf = this.generateTHLParallelPipeline(
                "testMultiChannelLag", 30, 50, 100);
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(),
                ReplicatorMonitor.getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Fetch references to stores.
        THL thl = (THL) pipeline.getStore("thl");
        InMemoryMultiQueue mq = (InMemoryMultiQueue) pipeline
                .getStore("multi-queue");

        // Write a large number of events into the THL using only 3 shards.
        String[] shardNames = {"db01", "db07", "db09"};
        LogConnection conn = thl.connect(false);
        long seqno = 0;
        for (int i = 0; i < 100000; i++)
        {
            for (int shard = 0; shard < 3; shard++)
            {
                ReplDBMSEvent rde = this
                        .createEvent(seqno++, shardNames[shard]);
                THLEvent thlEvent = new THLEvent(rde.getSourceId(), rde);
                conn.store(thlEvent, false);
            }
        }
        conn.commit();
        thl.disconnect(conn);

        // Read across each queue until we reach 100K events for the main
        // shards (i.e., 300K total). Time out after 60 seconds to avoid hangs.
        long startMillis = System.currentTimeMillis();
        int shardTotal = 0;
        while (shardTotal < 300000)
        {
            // Iterate across all queues.
            for (int q = 0; q < 30; q++)
            {
                // If the current queue has something in it...
                while (mq.peek(q) != null)
                {
                    // Read next event from this queue.
                    ReplDBMSEvent event = mq.get(q);
                    String shard = event.getShardId();

                    // If it's from a shard we are tracking, count it.
                    for (String shardName : shardNames)
                    {
                        if (shardName.equals(shard))
                            shardTotal++;

                        if (shardTotal % 30000 == 0)
                            logger.info("Tracked shard entries read:"
                                    + shardTotal);
                    }
                }

                // Check time.
                long testTimeMillis = System.currentTimeMillis() - startMillis;
                if (testTimeMillis > 150000)
                    throw new Exception("Took way too long to read shards!: "
                            + (testTimeMillis / 1000.0) + "s");
            }
        }

        // Close down pipeline.
        pipeline.shutdown(false);
        runtime.release();
    }

    // Returns the current serialization count from a parallel queue.
    private int getSerializationCount(THLParallelQueue tpq)
    {
        TungstenProperties props = tpq.status();
        return props.getInt("serializationCount");
    }

    // Generate configuration properties for a three stage-pipeline
    // that loads events into a THL then loads a parallel queue. Input
    // is from a dummy extractor.
    public TungstenProperties generateTHLParallelQueueProps(String schemaName,
            int channels) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "extract,feed,apply", "thl,thl-queue");
        builder.addStage("extract", "dummy", "thl-apply", null);
        builder.addStage("feed", "thl-extract", "thl-queue-apply", null);
        builder.addStage("apply", "thl-queue-extract", "dummy", null);

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "5");

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags", "1");
        builder.addComponent("applier", "thl-apply", THLStoreApplier.class);
        builder.addProperty("applier", "thl-apply", "storeName", "thl");

        // Feed stage components.
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Apply stage components.
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    // Generate configuration properties for a two-stage pipeline that
    // connects a THL to a THLParallelQueue to an in-memory multi queue, which
    // can mimic parallel apply on DBMS instances. Clients use direct calls
    // to the stores to write to and read from the pipeline.
    public TungstenProperties generateTHLParallelPipeline(String schemaName,
            int partitions, int blockCommit, int mqSize) throws Exception
    {
        // Clear the THL log directory.
        prepareLogDir(schemaName);

        // Convert values to strings so we can use them.
        String partitionsAsString = new Integer(partitions).toString();
        String blockCommitAsString = new Integer(blockCommit).toString();

        // Create pipeline.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA, schemaName);
        builder.addPipeline("master", "feed1, feed2",
                "thl,thl-queue, multi-queue");

        // Define stores.
        builder.addComponent("store", "thl", THL.class);
        builder.addProperty("store", "thl", "logDir", schemaName);
        builder.addComponent("store", "thl-queue", THLParallelQueue.class);
        builder.addProperty("store", "thl-queue", "maxSize", "100");
        builder.addProperty("store", "thl-queue", "partitions", new Integer(
                partitions).toString());
        builder.addProperty("store", "thl-queue", "partitionerClass",
                HashPartitioner.class.getName());
        builder.addComponent("store", "multi-queue", InMemoryMultiQueue.class);
        builder.addProperty("store", "multi-queue", "maxSize", new Integer(
                mqSize).toString());
        builder.addProperty("store", "multi-queue", "partitions",
                partitionsAsString);

        // Feed1 stage components.
        builder.addStage("feed1", "thl-extract", "thl-queue-apply", null);
        builder.addProperty("stage", "feed1", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-extract",
                THLStoreExtractor.class);
        builder.addProperty("extractor", "thl-extract", "storeName", "thl");
        builder.addComponent("applier", "thl-queue-apply",
                THLParallelQueueApplier.class);
        builder.addProperty("applier", "thl-queue-apply", "storeName",
                "thl-queue");

        // Feed2 stage components.
        builder.addStage("feed2", "thl-queue-extract", "multi-queue-apply",
                null);
        builder.addProperty("stage", "feed2", "taskCount", partitionsAsString);
        builder.addProperty("stage", "feed2", "blockCommitRowCount",
                blockCommitAsString);
        builder.addComponent("extractor", "thl-queue-extract",
                THLParallelQueueExtractor.class);
        builder.addProperty("extractor", "thl-queue-extract", "storeName",
                "thl-queue");
        builder.addComponent("applier", "multi-queue-apply",
                InMemoryMultiQueueApplier.class);
        builder.addProperty("applier", "multi-queue-apply", "storeName",
                "multi-queue");

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