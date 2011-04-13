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
 * Initial developer(s): Robert Hodges, Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.filter;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * Ignores or replicates a database using rules similar to MySQL ignore-db and
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ShardFilter implements Filter
{
    private static Logger logger = Logger.getLogger(ShardFilter.class);

    private String        doShard;
    
    @SuppressWarnings("unused")
    // TODO should this field be removed ?
    private String        ignoreShard;

    /**
     * Sets a list of one or more shards to replicate. Shard names are
     * comma-separated.
     */
    public void setDoShard(String doShard)
    {
        this.doShard = doShard;
    }

    /**
     * Sets a list of one or more databases to replicate. Database names are
     * comma-separated.
     */
    public void setIgnoreShard(String ignoreShard)
    {
        this.ignoreShard = ignoreShard;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        String shardId = event.getDBMSEvent()
                .getMetadataOptionValue(ReplOptionParams.SHARD_ID);
        if (shardId.equals(doShard))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Accepting event: seqno=" + event.getSeqno() + " shard_id=" + shardId);
            }
            return event;
        }
        else
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Dropping event: seqno=" + event.getSeqno() + " shard_id=" + shardId);
            }
            return null;
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }
}
