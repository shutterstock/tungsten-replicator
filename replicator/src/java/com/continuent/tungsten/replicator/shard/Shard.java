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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.shard;

import java.util.Map;

/**
 * Holds information about a single shard, whose name is given by the shardId
 * property.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class Shard
{
    /**
     * Master name that denotes a short that is local to current service only.
     */
    public String   LOCAL = "#LOCAL";

    // Shard properties.
    private String  shardId;
    private boolean critical;
    private String  master;

    public Shard(String shardId, boolean critical, String master)
    {
        this.shardId = shardId;
        this.critical = critical;
        this.master = master;
    }

    public Shard(Map<String, String> shard)
    {
        this.shardId = shard.get(ShardTable.SHARD_ID_COL);
        this.critical = Boolean.valueOf(shard.get(ShardTable.SHARD_CRIT_COL));
        this.master = shard.get(ShardTable.SHARD_MASTER_COL);
    }

    /** Returns the shard name. */
    public String getShardId()
    {
        return shardId;
    }

    /** Returns true if shard is critical. */
    public boolean isCritical()
    {
        return critical;
    }

    /** Returns name of master service. */
    public String getMaster()
    {
        return master;
    }

    /**
     * Returns true if shard is local-only, i.e., does not cross services.
     */
    public boolean isLocal()
    {
        return LOCAL.equals(master);
    }
}
