/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.database;

import com.continuent.tungsten.commons.cache.CacheResourceManager;
import com.continuent.tungsten.commons.cache.IndexedLRUCache;

/**
 * Implements a cache for table metadata. The cache organizes Table metadata by
 * schema and table name. It supports invalidation at multiple levels.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class TableMetadataCache implements CacheResourceManager<Table>
{
    IndexedLRUCache<Table> cache;

    /**
     * Creates a new table metadata cache.
     */
    public TableMetadataCache(int capacity)
    {
        cache = new IndexedLRUCache<Table>(capacity, this);
    }

    /**
     * Call back to release a table metadata instance that is dropped from the
     * cache.
     * 
     * @see com.continuent.tungsten.commons.cache.CacheResourceManager#release(java.lang.Object)
     */
    public void release(Table metadata)
    {
        // Do nothing.
    }

    /**
     * Returns the number of entries in the metadata cache.
     */
    public int size()
    {
        return cache.size();
    }

    /**
     * Store metadata for a table.
     */
    public void store(Table metadata)
    {
        String key = generateKey(metadata.getSchema(), metadata.getName()); 
        cache.put(key, metadata);
    }

    /**
     * Retrieves table metadata or returns null if it is not in the cache.
     */
    public Table retrieve(String schema, String tableName)
    {
        String key = generateKey(schema, tableName);
        return cache.get(key);
    }

    /**
     * Release all metadata in the cache.
     */
    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    /**
     * Release all table metadata instances for a given schema.
     */
    public int invalidateSchema(String schema)
    {
        return cache.invalidateByPrefix(schema);
    }

    /**
     * Release all a single table metadata instance
     */
    public int invalidateTable(String schema, String tableName)
    {
        String key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }

    /**
     * Invalidate appropriate range of metadata based on a particular SQL
     * operation that we see.
     * 
     * @param sqlOperation A SQLOperation from parsing
     * @param defaultSchema Default schema in case it is not supplied by
     *            sqlOperation
     */
    public int invalidate(SqlOperation sqlOperation, String defaultSchema)
    {
        if (sqlOperation.getOperation() == SqlOperation.DROP
                && sqlOperation.getObjectType() == SqlOperation.SCHEMA)
        {
            return cache.invalidateByPrefix(sqlOperation.getSchema());
        }
        else if (sqlOperation.getOperation() == SqlOperation.DROP
                && sqlOperation.getObjectType() == SqlOperation.TABLE)
        {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema,
                    sqlOperation.getName());
        }
        else if (sqlOperation.getOperation() == SqlOperation.ALTER)
        {
            return invalidateTable(sqlOperation.getSchema(), defaultSchema,
                    sqlOperation.getName());
        }
        return 0;

    }

    // Generate a key for table.
    private String generateKey(String schema, String tableName)
    {
        StringBuffer key = new StringBuffer();
        key.append(schema);
        key.append(".");
        key.append(tableName);
        return key.toString();
    }

    // Utility method to drop table metadata.
    private int invalidateTable(String schema, String defaultSchema,
            String tableName)
    {
        String key;
        if (schema == null)
            key = generateKey(defaultSchema, tableName);
        else
            key = generateKey(schema, tableName);
        return cache.invalidate(key);
    }
}