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

/**
 * Contains partitioning response data.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PartitionerResponse
{
    private final int     partition;
    private final boolean critical;

    /**
     * Generates a new response.
     * 
     * @param partition Partition to which current event should be assigned
     * @param critical If true, this event requires a critical section
     */
    public PartitionerResponse(int partition, boolean critical)
    {
        this.partition = partition;
        this.critical = critical;
    }

    public int getPartition()
    {
        return partition;
    }

    public boolean isCritical()
    {
        return critical;
    }
}