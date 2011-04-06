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

package com.continuent.tungsten.enterprise.replicator.pipeline;

import java.sql.Timestamp;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.enterprise.replicator.store.ParallelQueueApplier;
import com.continuent.tungsten.enterprise.replicator.store.ParallelQueueExtractor;
import com.continuent.tungsten.enterprise.replicator.store.ParallelQueueStore;
import com.continuent.tungsten.enterprise.replicator.store.RoundRobinPartitioner;
import com.continuent.tungsten.replicator.EventDispatcher;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.PipelineConfigBuilder;

/**
 * Verifies parallel stage processing. Cases check parallel processing using
 * multiple tasks in a single stage.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ParallelStageTest extends TestCase
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
     * Confirm that we can replicate using multiple tasks between parallel
     * stores.
     */
    public void testBasicParallelStage() throws Exception
    {
        // Generate configuration with parallel stores at either
        // end. There are three queues in each store, and three
        // tasks in the stage. Max queue size is 3 events.
        TungstenProperties conf = this.getConfig(3, 3, 3, 3);

        // Generate context and configure pipeline.
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(), ReplicatorMonitor
                        .getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Load 3 events into each queue.
        ParallelQueueStore inputPqs = (ParallelQueueStore) pipeline
                .getStore("input");
        assertEquals("3 input partitions defined", 3, inputPqs.getPartitions());
        for (int i = 0; i < 3 * 3; i++)
        {
            ReplDBMSEvent event = createEvent(i);
            inputPqs.put(i % 3, event);
        }

        // Start the pipeline.
        pipeline.start(new EventDispatcher());

        // Fetch events back out of the output store, to which they should
        // transfer automatically.
        ParallelQueueStore outputPqs = (ParallelQueueStore) pipeline
                .getStore("output");
        assertEquals("3 output partitions defined", 3, outputPqs
                .getPartitions());
        for (int i = 0; i < 3 * 3; i++)
        {
            int partId = i % 3;
            ReplDBMSEvent event = (ReplDBMSEvent) outputPqs.get(partId);
            assertEquals("Event event has expected seqno", i, event.getSeqno());
        }

        // Stop and release the pipeline.
        pipeline.shutdown(true);
        pipeline.release(runtime);
    }

    /**
     * Confirm that we can assign events into multiple queues using round- robin
     * partitioning, which assigns events from the stage into queues by setting
     * each successive seqno to the next partition.
     */
    public void testRoundRobinParallelStore() throws Exception
    {
        // Generate configuration with parallel stores at either
        // end. There are three queues in each store, and three
        // tasks in the stage. Max queue size is 12 events. We
        // assign the round-robin partitioner explicitly to
        // output queue.
        TungstenProperties conf = this.getConfig(1, 1, 3, 12);
        conf.setString("replicator.store.output.partitionerClass",
                RoundRobinPartitioner.class.getName());

        // Generate context and configure pipeline.
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(), ReplicatorMonitor
                        .getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();

        // Configure stores to suppress sync events.
        ParallelQueueStore inputPqs = (ParallelQueueStore) pipeline
                .getStore("input");
        inputPqs.setSyncInterval(100);
        ParallelQueueStore outputPqs = (ParallelQueueStore) pipeline
                .getStore("output");
        outputPqs.setSyncInterval(100);

        // Load 12 events into the input queue store, which has a
        // single partition.
        assertEquals("1 input partition defined", 1, inputPqs.getPartitions());
        for (int i = 0; i < 12; i++)
        {
            ReplDBMSEvent event = createEvent(i);
            inputPqs.put(0, event);
        }

        // Start the pipeline.
        pipeline.start(new EventDispatcher());

        // Fetch events back out of the output store, to which they should
        // transfer automatically.
        assertEquals("3 output partitions defined", 3, outputPqs
                .getPartitions());
        for (int i = 0; i < 12; i++)
        {
            int partId = i % 3;
            ReplEvent raw = outputPqs.get(partId);
            ReplDBMSEvent event = (ReplDBMSEvent) raw;
            assertEquals("Event event has expected seqno", i, event.getSeqno());
        }

        // Stop and release the pipeline.
        pipeline.shutdown(true);
        pipeline.release(runtime);
    }

    private TungstenProperties getConfig(int inputPartitions, int taskCount,
            int outputPartitions, int queueSize) throws Exception
    {
        // Convert int values to strings.
        String inputPartitionsAsString = new Integer(inputPartitions)
                .toString();
        String taskCountAsString = new Integer(taskCount).toString();
        String outputPartitionsAsString = new Integer(outputPartitions)
                .toString();
        String queueSizeAsString = new Integer(queueSize).toString();

        // Build configuration properties.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.setProperty(ReplicatorConf.METADATA_SCHEMA,
                "testBasicParallelStage");
        builder.addPipeline("master", "transfer", "input,output");
        builder
                .addStage("transfer", "parallel-extract", "parallel-apply",
                        null);
        builder
                .addProperty("stage", "transfer", "taskCount",
                        taskCountAsString);

        builder.addComponent("extractor", "parallel-extract",
                ParallelQueueExtractor.class);
        builder.addProperty("extractor", "parallel-extract", "storeName",
                "input");
        builder.addComponent("applier", "parallel-apply",
                ParallelQueueApplier.class);
        builder.addProperty("applier", "parallel-apply", "storeName", "output");

        builder.addComponent("store", "input", ParallelQueueStore.class);
        builder.addProperty("store", "input", "partitions",
                inputPartitionsAsString);
        builder.addProperty("store", "input", "maxSize", queueSizeAsString);
        builder.addComponent("store", "output", ParallelQueueStore.class);
        builder.addProperty("store", "output", "partitions",
                outputPartitionsAsString);
        builder.addProperty("store", "output", "maxSize", queueSizeAsString);
        TungstenProperties conf = builder.getConfig();
        return conf;
    }

    // Returns a well-formed ReplDBMSEvent.
    private ReplDBMSEvent createEvent(long seqno)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, true, new Timestamp(System.currentTimeMillis()));
        return new ReplDBMSEvent(seqno, dbmsEvent);
    }
}
