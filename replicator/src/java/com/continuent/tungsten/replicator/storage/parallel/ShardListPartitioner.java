/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.storage.parallel;

import java.io.File;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.channel.ChannelAssignmentService;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.service.PipelineService;

/**
 * Partitions events using a map that directs shard assignment to partition
 * numbers. The default shard map location is by convention
 * <code>tungsten-replicator/conf/shard.list</code>. The shard map structure
 * follows the example shown here.
 * 
 * <pre><code> # Shard map file. 
 * # Explicit database name match. 
 * common1=0
 * common2=0
 * db1=1
 * db2=2
 * db3=3
 * 
 * # Default partition for shards that do not match explicit name. 
 * # Permissible values are either a partition number or -1 in 
 * # which case values are hashed across available partitions. 
 * (*)=4
 * 
 * # Comma-separated list of shards that require critical section to run. 
 * (critical)=common1,common2
 * 
 * # Method for channel hash assignments.  Allowed values are round-robin and 
 * # string-hash. 
 * (hash-method)=string-hash
 * </code></pre>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardListPartitioner implements Partitioner
{
    private static Logger            logger           = Logger.getLogger(ShardListPartitioner.class);

    private int                      availablePartitions;

    // Hashing methods.
    private static int               STRING_HASH      = 1;
    private static int               ROUND_ROBIN      = 2;

    // Control fields.
    private PluginContext            context;
    private File                     shardMap;
    private HashMap<String, Integer> shardTable;
    private int                      defaultPartition = -1;
    private HashMap<String, Boolean> criticalShards;
    private int                      hashMethod       = STRING_HASH;

    // Shard/channel assignment service
    private ChannelAssignmentService channelAssignmentService;

    /**
     * Create new instance of partitioner.
     */
    public ShardListPartitioner()
    {
    }

    public synchronized void setShardMap(File shardMap)
    {
        this.shardMap = shardMap;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setPartitions(int)
     */
    public synchronized void setPartitions(int availablePartitions)
    {
        this.availablePartitions = availablePartitions;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#setContext(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void setContext(PluginContext context)
    {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#partition(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      int, int)
     */
    public synchronized PartitionerResponse partition(ReplDBMSHeader event, int taskId)
            throws ReplicatorException
    {
        // Initialize on first call.
        if (shardTable == null)
            initialize();

        // See if there is an explicit partition assignment.
        String shardId = event.getShardId();
        Integer partition = shardTable.get(shardId);

        // If not, either assign to the default partition or hash.
        if (partition == null)
        {
            if (defaultPartition >= 0)
                partition = new Integer(defaultPartition);
            else if (this.hashMethod == STRING_HASH)
                partition = new Integer(Math.abs(shardId.hashCode())
                        % availablePartitions);
            else if (hashMethod == ROUND_ROBIN)
            {
                // If we don't know about this shard, add a entry to our shard
                // table.
                if (shardTable.get(shardId) == null)
                {
                    // See if the channel assignment service knows about it.
                    Integer newPartition = channelAssignmentService
                            .getChannelAssignment(shardId);

                    // Add entry to our local table.
                    shardTable.put(shardId, newPartition);
                }

                // Now assign the partition.
                partition = shardTable.get(shardId);
            }
        }

        // Compute whether this is a critical shard.
        boolean critical = (criticalShards.get(shardId) != null || ReplOptionParams.SHARD_ID_UNKNOWN
                .equals(shardId));

        // Finally, return a response.
        return new PartitionerResponse(partition, critical);
    }

    // Find and load the shard table.
    private void initialize() throws ReplicatorException
    {
        // If the shard map file is not set, try to set it now. This default
        // works in the replicator though not necessary when running in a
        // unit test.
        if (shardMap == null)
        {
            File replicatorConfDir = ReplicatorRuntimeConf
                    .locateReplicatorConfDir();
            shardMap = new File(replicatorConfDir, "shard.list");
        }

        // Load shard properties.
        TungstenProperties shardMapProperties = PartitionerUtility
                .loadShardProperties(shardMap);

        // Construct data used for partitioning.
        logger.info("Loading shard partitioning data");
        shardTable = new HashMap<String, Integer>();
        criticalShards = new HashMap<String, Boolean>();

        // The #UNKNOWN shard must be declared critical or it will not be
        // processed correctly.
        criticalShards.put(ReplOptionParams.SHARD_ID_UNKNOWN, true);

        for (String key : shardMapProperties.keyNames())
        {
            if ("(*)".equals(key))
            {
                defaultPartition = shardMapProperties.getInt(key);
            }
            else if ("(critical)".equals(key))
            {
                logger.info("Setting critical shards: "
                        + shardMapProperties.getString(key));
                List<String> criticalShardList = shardMapProperties
                        .getStringList(key);
                for (String criticalShard : criticalShardList)
                {
                    criticalShards.put(criticalShard, true);
                }
            }
            else if ("(hash-method)".equals(key))
            {
                String method = shardMapProperties.getString(key);
                if ("string-hash".equals(method))
                {
                    hashMethod = STRING_HASH;
                }
                else if ("round-robin".equals(method))
                {
                    hashMethod = ROUND_ROBIN;
                }
                else
                {
                    throw new ReplicatorException(
                            "Unknown hashing method; valid methods are string-hash or round-robin: "
                                    + method);
                }
            }
            else
            {
                int partition = shardMapProperties.getInt(key);
                shardTable.put(key, partition);
            }
        }

        // Report default partition.
        if (defaultPartition >= 0)
        {
            logger.info("Default partition specified: " + defaultPartition);
        }
        else
        {
            logger.info("No default partition specified; unassigned shards will use hashing");
        }

        // Load shard assignments if using round-robin hashing. String method =
        // shardMapProperties.getString(key);
        if (hashMethod == STRING_HASH)
        {
            logger.info("Using string hashing for channel assignment");
        }
        else if (hashMethod == ROUND_ROBIN)
        {
            logger.info("Using persistent round-robin hashing for channel assignment");
            PipelineService svc = context.getService("channel-assignment");
            if (svc == null)
            {
                throw new ReplicatorException(
                        "Unable to find required channel-assignment service to manage channels");
            }
            else if (!(svc instanceof ChannelAssignmentService))
            {
                throw new ReplicatorException(
                        "Incorrect class type for channel-assignment service: required="
                                + ChannelAssignmentService.class.getName()
                                + " actual=" + svc.getClass().getName());
            }
            else
            {
                channelAssignmentService = (ChannelAssignmentService) svc;
                logger.info("Channel assignment service loaded: " + svc.getName());
            }
        }

        // Dump shard assignment table in debug mode.
        if (logger.isDebugEnabled())
            logger.debug("Shard table: " + shardTable.toString());
    }
}