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

import java.sql.Timestamp;
import java.util.ArrayList;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.DummyExtractor;
import com.continuent.tungsten.replicator.storage.InMemoryQueueAdapter;
import com.continuent.tungsten.replicator.storage.InMemoryQueueStore;

/**
 * This class contains utility functions for pipeline tests.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineHelper
{
    /**
     * Generate a simple configuration with a single stage.
     */
    public TungstenProperties createSimpleRuntime() throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "master", null);
        builder.addStage("master", "dummy", "dummy", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    /**
     * Generate a simple runtime configuration that will process a specified
     * number of transactions.
     * 
     * @return
     * @throws Exception
     */
    public TungstenProperties createSimpleRuntimeWithXacts(int nTrx)
            throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "master", null);
        builder.addStage("master", "dummy", "dummy", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nTrx",
                new Integer(nTrx).toString());
        builder.addComponent("applier", "dummy", DummyApplier.class);

        return builder.getConfig();
    }

    /**
     * Generate a simple runtime configuration with specific services.
     * 
     * @return
     * @throws Exception
     */
    public TungstenProperties createSimpleRuntimeWith2Services()
            throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "master", null, "svc1,svc2");
        builder.addStage("master", "dummy", "dummy", null);
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addComponent("applier", "dummy", DummyApplier.class);

        // Service definitions.
        builder.addComponent("service", "svc1", SampleService.class);
        builder.addComponent("service", "svc2", SampleService.class);

        return builder.getConfig();
    }

    /**
     * Generate a simple runtime with a queue on both ends of a simple task.
     * 
     * @return
     * @throws Exception
     */
    public TungstenProperties createDoubleQueueRuntime(int queueSize,
            int blockSize) throws Exception
    {
        // Overall pipeline including one stage with block size.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "stage", "q1,q2");
        builder.addStage("stage", "q-extract", "q-apply", null);
        builder.addProperty("stage", "stage", "blockCommitRowCount",
                new Integer(blockSize).toString());

        // Stage components.
        builder.addComponent("extractor", "q-extract",
                InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "q-extract", "storeName", "q1");
        builder.addComponent("applier", "q-apply", InMemoryQueueAdapter.class);
        builder.addProperty("applier", "q-apply", "storeName", "q2");

        // Storage definition.
        builder.addComponent("store", "q1", InMemoryQueueStore.class);
        builder.addProperty("store", "q1", "maxSize",
                new Integer(queueSize).toString());
        builder.addComponent("store", "q2", InMemoryQueueStore.class);
        builder.addProperty("store", "q2", "maxSize",
                new Integer(queueSize).toString());

        return builder.getConfig();
    }

    /**
     * Generate a simple runtime with a queue on both ends of a simple task that
     * has a sample filter.
     * 
     * @return
     * @throws Exception
     */
    public TungstenProperties createDoubleQueueWithFilter(int queueSize,
            int blockSize, long skipSeqnoStart, long skipSeqnoRange,
            boolean skipSeqnoMultiple) throws Exception
    {
        // Overall pipeline including one stage with block size.
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "stage", "q1,q2");
        builder.addStage("stage", "q-extract", "q-apply", "sample-filter");
        builder.addProperty("stage", "stage", "blockCommitRowCount",
                new Integer(blockSize).toString());

        // Stage components.
        builder.addComponent("extractor", "q-extract",
                InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "q-extract", "storeName", "q1");
        builder.addComponent("applier", "q-apply", InMemoryQueueAdapter.class);
        builder.addProperty("applier", "q-apply", "storeName", "q2");

        // Storage definition.
        builder.addComponent("store", "q1", InMemoryQueueStore.class);
        builder.addProperty("store", "q1", "maxSize",
                new Integer(queueSize).toString());
        builder.addComponent("store", "q2", InMemoryQueueStore.class);
        builder.addProperty("store", "q2", "maxSize",
                new Integer(queueSize).toString());

        // Filter definition.
        builder.addComponent("filter", "sample-filter", SampleFilter.class);
        builder.addProperty("filter", "sample-filter", "skipSeqnoStart",
                new Long(skipSeqnoStart).toString());
        builder.addProperty("filter", "sample-filter", "skipSeqnoRange",
                new Long(skipSeqnoRange).toString());
        builder.addProperty("filter", "sample-filter", "skipSeqnoMultiple",
                new Boolean(skipSeqnoMultiple).toString());

        return builder.getConfig();
    }

    /**
     * Generate a runtime configuration with a store. Fragmentation numbers
     * above 0 generate fragmented events.
     */
    public TungstenProperties createRuntimeWithStore(int nfrags)
            throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "extract, apply", "queue");
        builder.addStage("extract", "dummy", "q-apply", null);
        builder.addStage("apply", "q-extract", "dummy", null);

        // Extract stage components.
        builder.addComponent("extractor", "dummy", DummyExtractor.class);
        builder.addProperty("extractor", "dummy", "nFrags",
                new Integer(nfrags).toString());
        builder.addComponent("applier", "q-apply", InMemoryQueueAdapter.class);
        builder.addProperty("applier", "q-apply", "storeName", "queue");

        // Apply stage components.
        builder.addComponent("extractor", "q-extract",
                InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "q-extract", "storeName", "queue");
        builder.addComponent("applier", "dummy", DummyApplier.class);

        // Storage definition.
        builder.addComponent("store", "queue", InMemoryQueueStore.class);
        builder.addProperty("store", "queue", "maxSize", "10");

        return builder.getConfig();
    }

    /**
     * Returns a well-formed ReplDBMSEvent with a specified shard ID.
     * 
     * @param seqno Sequence number to use
     * @param shardId Shard ID to use
     */
    public ReplDBMSEvent createEvent(long seqno, String shardId)
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

    /**
     * Returns a well-formed ReplDBMSEvent fragment with a specified shard ID.
     * 
     * @param seqno Sequence number to use
     * @param shardId Shard ID to use
     * @param fragNo Fragment number
     * @param lastFrag If true, this is the last fragment.
     */
    public ReplDBMSEvent createEvent(long seqno, String shardId, short fragNo,
            boolean lastFrag)
    {
        ArrayList<DBMSData> t = new ArrayList<DBMSData>();
        t.add(new StatementData("SELECT 1"));
        DBMSEvent dbmsEvent = new DBMSEvent(new Long(seqno).toString(), null,
                t, lastFrag, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent replDbmsEvent = new ReplDBMSEvent(seqno, fragNo,
                lastFrag, "NONE", 0, new Timestamp(System.currentTimeMillis()),
                dbmsEvent);
        replDbmsEvent.getDBMSEvent().addMetadataOption(
                ReplOptionParams.SHARD_ID, shardId);
        return replDbmsEvent;
    }
}