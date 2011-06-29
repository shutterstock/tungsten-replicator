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
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplOptionParams;

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
 * </code></pre>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardListPartitioner implements Partitioner
{
    private int                      availablePartitions;

    private static Logger            logger           = Logger.getLogger(ShardListPartitioner.class);
    private File                     shardMap;
    private HashMap<String, Integer> shardTable;
    private int                      defaultPartition = -1;
    private HashMap<String, Boolean> criticalShards;

    /**
     * Create new instance of partitioner.
     */
    public ShardListPartitioner()
    {
    }

    public void setShardMap(File shardMap)
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
     * @see com.continuent.tungsten.replicator.storage.parallel.Partitioner#partition(com.continuent.tungsten.replicator.event.ReplDBMSEvent,
     *      int, int)
     */
    public PartitionerResponse partition(ReplDBMSHeader event, int taskId)
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
            else
                partition = new Integer(Math.abs(shardId.hashCode())
                        % availablePartitions);
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

        for (String shardName : shardMapProperties.keyNames())
        {
            if ("(*)".equals(shardName))
            {
                defaultPartition = shardMapProperties.getInt(shardName);
            }
            else if ("(critical)".equals(shardName))
            {
                logger.info("Setting critical shards: "
                        + shardMapProperties.getString(shardName));
                List<String> criticalShardList = shardMapProperties
                        .getStringList(shardName);
                for (String criticalShard : criticalShardList)
                {
                    criticalShards.put(criticalShard, true);
                }
            }
            else
            {
                int partition = shardMapProperties.getInt(shardName);
                this.shardTable.put(shardName, partition);
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

        // Dump shard table in debug mode.
        if (logger.isDebugEnabled())
            logger.debug("Shard table: " + shardTable.toString());
    }
}