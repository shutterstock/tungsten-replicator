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

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.EventDispatcher;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;

/**
 * This class implements a test of pipeline monitoring functions.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class PipelineProgressTest extends TestCase
{
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
     * Verify that pipelines with no events return default values.
     */
    public void testPipelineWithNoEvents() throws Exception
    {
        // Create pipeline.
        TungstenProperties config = helper.createSimpleRuntimeWithXacts(0);
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(), ReplicatorMonitor
                        .getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Check a selection of default values.
        assertEquals("default latency", 0.0, pipeline.getApplyLatency());
        assertNull("default applied event", pipeline.getLastAppliedEvent());
        assertEquals("default applied seqno", -1, pipeline
                .getLastAppliedSeqno());
        assertEquals("default extracted seqno", -1, pipeline
                .getLastExtractedSeqno());

        // Shard list should be empty.
        List<ShardProgress> shards = pipeline.getShardProgress();
        assertEquals("empty shard list", 0, shards.size());

        // Pipeline should have one task.
        List<TaskProgress> tasks = pipeline.getTaskProgress();
        assertEquals("single task in list", 1, tasks.size());
        TaskProgress task = tasks.get(0);
        assertNull("default applied event", task.getLastEvent());
        assertEquals("no events processed on task", 0, task.getEventCount());

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }

    /**
     * Verify that the pipeline tracks processed sequence numbers.
     */
    public void testPipelineWithEvents() throws Exception
    {
        TungstenProperties config = helper.createSimpleRuntime();
        ReplicatorRuntime runtime = new ReplicatorRuntime(config,
                new MockOpenReplicatorContext(), ReplicatorMonitor
                        .getInstance());
        runtime.configure();
        runtime.prepare();
        Pipeline pipeline = runtime.getPipeline();
        pipeline.start(new EventDispatcher());

        // Wait for and verify events.
        Future<ReplDBMSHeader> wait = pipeline.watchForAppliedSequenceNumber(9);
        ReplDBMSHeader lastEvent = wait.get(10, TimeUnit.SECONDS);
        assertEquals("Expected 10 sequence numbers", 9, lastEvent.getSeqno());

        // Check a selection of default values.
        assertEquals("last applied event", lastEvent, pipeline
                .getLastAppliedEvent());
        assertEquals("default applied seqno", 9, pipeline.getLastAppliedSeqno());
        assertEquals("default extracted seqno", 9, pipeline
                .getLastExtractedSeqno());

        // Shard list should be empty.
        List<ShardProgress> shards = pipeline.getShardProgress();
        assertEquals("empty shard list", 1, shards.size());

        // Tasks should have a single task.
        List<TaskProgress> tasks = pipeline.getTaskProgress();
        assertEquals("single task in list", 1, tasks.size());
        TaskProgress task = tasks.get(0);
        assertEquals("applied event", lastEvent, task.getLastEvent());
        assertEquals("events processed on task", 10, task.getEventCount());

        // Shut down.
        pipeline.shutdown(false);
        pipeline.release(runtime);
    }
}