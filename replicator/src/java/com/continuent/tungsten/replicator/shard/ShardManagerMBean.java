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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Defines API for shard management extensions.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public interface ShardManagerMBean
{
    /**
     * Returns true so that clients can confirm connection liveness.
     * 
     * @return true if the service is up and running, false otherwise
     */
    public boolean isAlive();

    /**
     * Inserts a list of shards into the shard table. Each shard will be a map
     * of name-value parameters, for example, name -> my_shard, channel -> 2.
     * 
     * @param params a list of shards to be inserted
     * @return 
     * @throws SQLException 
     */
    public int insert(List<Map<String, String>> params) throws SQLException;

    /**
     * Updates a list of shards into the shard table. Each shard will be a map
     * of name-value parameters, for example, name -> my_shard, channel -> 2,
     * the key to be used to update being the shard name.
     * 
     * @param params a list of shards to be updated
     * @return 
     * @throws SQLException 
     */
    public int update(List<Map<String, String>> params) throws SQLException;

    /**
     * Deletes a list of shards based on shard ids (aka shard name). The list
     * will only contain shard ids.
     * 
     * @param params
     * @throws SQLException 
     */
    public int delete(List<Map<String, String>> params) throws SQLException;

    /**
     * Deletes all shards from the shard table.
     * @throws SQLException 
     */
    public int deleteAll() throws SQLException;

    /**
     * List all shards definitions
     * @throws SQLException 
     * 
     * @returns A list of shards represented by maps of name-value.
     */
    public List<Map<String, String>> list() throws SQLException;

}