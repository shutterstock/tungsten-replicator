/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;

/**
 * Implements a simple shard partitioner that hashes on the shard name.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class HashPartitioner implements Partitioner
{
    private int availablePartitions;

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
        if (taskId > availablePartitions)
            throw new ReplicatorException(
                    "Task ID exceeds available partitions: taskId=" + taskId
                            + " availablePartitions=" + availablePartitions);
        int partition = new Integer(Math.abs(event.getShardId().hashCode())
                % availablePartitions);
        return new PartitionerResponse(partition, false);
    }
}