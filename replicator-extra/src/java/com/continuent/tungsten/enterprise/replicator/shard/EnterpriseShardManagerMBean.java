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
 */

package com.continuent.tungsten.enterprise.replicator.shard;

import java.util.HashMap;
import java.util.List;

import com.continuent.tungsten.commons.jmx.DynamicMBeanHelper;

/**
 * Defines the shard manager administrative interface.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface EnterpriseShardManagerMBean
{
    /**
     * Detects physical shards from existing databases. 
     * 
     * @param dbIncludeExpression Pattern match for databases to include
     * @param dbExcludeExpression Pattern match for databases to exclude
     * @param enabled If true, new shards are enabled immediately
     * @return A list of newly created shards
     * @throws Exception
     */
    public List<HashMap<String, String>> detect(String dbIncludeExpression,
            String dbExcludeExpression, boolean enabled) throws Exception;

    /**
     * Creates a shard and sets its initial state.
     * 
     * @param shardId The unique shard ID
     * @param enabled If true the shard is enabled on creation
     * @throws Exception Thrown if shard cannot be created
     */
    public void create(String shardId, boolean enabled) throws Exception;

    /**
     * Drops all shards matching a particular shard expression.
     * 
     * @param shardExpression An expression including optional wild cards
     * @return True if one or more matching shards are found and dropped
     * @throws Exception Thrown if shard(s) cannot be dropped
     */
    public boolean drop(String shardExpression) throws Exception;

    /**
     * Enables one or more shards matching the shard ID expression
     * 
     * @param shardExpression An expression matching one or more shard IDs
     */
    public void enable(String shardExpression) throws Exception;

    /**
     * Disables one or more shards matching the shard ID expression
     * 
     * @param shardExpression An expression matching one or more shard IDs
     */
    public void disable(String shardExpression) throws Exception;

    /**
     * Fetch a list of currently defined shards and their state. Shards are
     * represented using HashMaps containing property values.
     * 
     * @return A list of defined shards
     */
    public List<HashMap<String, String>> list() throws Exception;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    public abstract DynamicMBeanHelper createHelper() throws Exception;
}