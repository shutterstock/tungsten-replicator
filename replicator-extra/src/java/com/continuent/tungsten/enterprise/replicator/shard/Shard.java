/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * 
 * This code is property of Continuent, Inc.  All rights reserved. 
 */

package com.continuent.tungsten.enterprise.replicator.shard;

/**
 * Implements a shard definition.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class Shard
{
    private String     shardId;
    private ShardState shardState;

    /**
     * Instantiates a new shard manager
     */
    public Shard()
    {
    }

    /** Returns the unique shard ID. */
    public String getShardId()
    {
        return shardId;
    }

    /** Sets the unique shard ID. */
    public void setShardId(String shardId)
    {
        this.shardId = shardId;
    }

    /** Returns the current administrative state of the shard. */
    public ShardState getShardState()
    {
        return shardState;
    }

    /** Sets the current administrative state of the shard. */
    public void setShardState(ShardState shardState)
    {
        this.shardState = shardState;
    }
}