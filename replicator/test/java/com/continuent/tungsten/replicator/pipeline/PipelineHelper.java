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

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.applier.DummyApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
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
     * Generate a simple runtime with a queue on both ends of a simple task.
     * 
     * @return
     * @throws Exception
     */
    public TungstenProperties createDoubleQueueRuntime(int size)
            throws Exception
    {
        PipelineConfigBuilder builder = new PipelineConfigBuilder();
        builder.setProperty(ReplicatorConf.SERVICE_NAME, "test");
        builder.setRole("master");
        builder.addPipeline("master", "master", "q1,q2");
        builder.addStage("extract", "q-extract", "q-apply", null);

        // Stage components.
        builder.addComponent("extractor", "q-extract",
                InMemoryQueueAdapter.class);
        builder.addProperty("extractor", "q-extract", "storeName", "q1");
        builder.addComponent("applier", "q-apply", InMemoryQueueAdapter.class);
        builder.addProperty("applier", "q-apply", "storeName", "q2");

        // Storage definition.
        builder.addComponent("store", "q1", InMemoryQueueStore.class);
        builder.addProperty("store", "q1", "maxSize",
                new Integer(size).toString());
        builder.addComponent("store", "q2", InMemoryQueueStore.class);
        builder.addProperty("store", "q2", "maxSize",
                new Integer(size).toString());

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
}