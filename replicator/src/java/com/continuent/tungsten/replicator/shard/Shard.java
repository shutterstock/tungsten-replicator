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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.shard;

import java.util.Map;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class Shard
{

    public static enum ShardDisposition
    {
        ACCEPT, REJECT, WARN, ERROR
    }

    private String  shardId;
    private boolean critical;
    private String  disposition;
    private int     channel;
    private String  home;

    public Shard(String shardId, boolean critical, String home, int channel)
    {
        this.shardId = shardId;
        this.critical = critical;
        this.home = home;
        this.channel = channel;
    }

    public Shard(Map<String, String> shard)
    {
        this.shardId = shard.get(ShardTable.SHARD_ID_COL);
        this.critical = Boolean.valueOf(shard.get(ShardTable.SHARD_CRIT_COL));
        this.home = shard.get(ShardTable.SHARD_HOME_COL);
        this.channel = Integer.valueOf(shard.get(ShardTable.SHARD_CHANNEL_COL));
    }

    public String getShardId()
    {
        return shardId;
    }

    public boolean isCritical()
    {
        return critical;
    }

    public String getDisposition()
    {
        return disposition;
    }

    public int getChannel()
    {
        return channel;
    }

    public String getHome()
    {
        return home;
    }

}
