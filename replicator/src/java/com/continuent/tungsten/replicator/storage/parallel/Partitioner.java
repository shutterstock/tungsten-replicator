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

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Implements an algorithm to divide replicator events into partitions.  This
 * is used to support channel assignment in parallel apply.  The partitioning
 * algorithm must be idempotent and must result in the same channel assignment
 * for a specific shard.  The algorithm may change only after a clean 
 * shutdown, i.e., following an offline operation. <p/>
 * 
 * Channels were called "partitions" in the original parallel apply 
 * implementation.  The terms partition and channel are equivalent in the code. 
 * Only channel should be used for user-visible interfaces, configuration 
 * files, and documentation.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Partitioner
{
    /**
     * Sets the number of available partitions.
     * 
     * @param availablePartitions Number of partitions available
     */
    public void setPartitions(int availablePartitions);

    /**
     * Assigns the current runtime context in case the partitioner needs to
     * refer to replicator state.
     * 
     * @param context Replicator runtime context
     */
    public void setContext(PluginContext context);

    /**
     * Assign an event to a particular partition. All fragments of a particular
     * sequence number must go to the same partition.
     * 
     * @param event Event to be assigned a partition
     * @param taskId Task id of input thread
     * @return Response containing partition ID and whether event requires a
     *         critical section
     */
    public PartitionerResponse partition(ReplDBMSHeader event, int taskId)
            throws ReplicatorException;
}