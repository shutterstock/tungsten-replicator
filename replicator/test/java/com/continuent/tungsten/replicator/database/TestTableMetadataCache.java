/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Robert Hodges and Scott Martin
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import java.sql.Types;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the TableMetadataCache.  There is a small number of 
 * cases as the underlying IndexedLRUCache has its own unit tests. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestTableMetadataCache
{
    /**
     * TODO: setUp definition.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * TODO: tearDown definition.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Ensure we can cache and retrieve Table instances by their names.
     */
    @Test
    public void testCacheTables() throws Exception
    {
        String[] schemas = {"a", "b", "c"};
        String[] tableNames = {"x", "y", "z"};

        // Create 3 tables in 3 schemas.
        TableMetadataCache tmc = this.populateCache(schemas, tableNames);

        // Ensure we find the proper number of items in the cache.
        Assert.assertEquals("Expected cache size", 9, tmc.size());

        // Ensure we can fetch all tables back.
        int tab = 0;
        for (String schema : schemas)
            for (String tableName : tableNames)
            {
                Table t = tmc.retrieve(schema, tableName);
                Assert.assertNotNull("Found table", t);
                Assert.assertEquals("Schema name", schema, t.getSchema());
                Assert.assertEquals("Schema name", tableName, t.getName());
                tab++;
            }

        // Clear the cache.
        tmc.invalidateAll();
    }

    /**
     * Ensure we can invalidate tables using explicit names or SQL operations. 
     */
    @Test
    public void testInvalidation() throws Exception
    {
        // Create 3 tables in 3 schemas.
        String[] schemas = {"a", "b", "c"};
        String[] tableNames = {"x", "y", "z"};
        TableMetadataCache tmc = this.populateCache(schemas, tableNames);

        // Invalidate by table name. 
        int invalidated = tmc.invalidateTable("a", "x");
        Assert.assertEquals("Specific table", 1, invalidated);

        invalidated = tmc.invalidateTable("a", "x");
        Assert.assertEquals("Specific table", 0, invalidated);

        // Invalidate by database. 
        invalidated = tmc.invalidateSchema("a");
        Assert.assertEquals("Specific schema", 2, invalidated);

        invalidated = tmc.invalidateSchema("a");
        Assert.assertEquals("Specific schema", 0, invalidated);
        
        // Invalidate by DROP DATABASE. 
        SqlOperation op = new SqlOperation(SqlOperation.SCHEMA, SqlOperation.DROP, "b", null);
        invalidated = tmc.invalidate(op, "a");
        Assert.assertEquals("drop database", 3, invalidated);

        invalidated = tmc.invalidate(op, "a");
        Assert.assertEquals("drop database", 0, invalidated);

        // Invalidate by DROP TABLE. 
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.DROP, "c", "x");
        invalidated = tmc.invalidate(op, "d");
        Assert.assertEquals("drop table", 1, invalidated);

        invalidated = tmc.invalidate(op, "d");
        Assert.assertEquals("drop table", 0, invalidated);

        // Invalidate by ALTER TABLE. 
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.ALTER, null, "y");
        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 1, invalidated);

        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 0, invalidated);
        
        // Not invalidated by an insert. 
        op = new SqlOperation(SqlOperation.TABLE, SqlOperation.INSERT, "c", "z");
        invalidated = tmc.invalidate(op, "c");
        Assert.assertEquals("alter table", 0, invalidated);

        // Clear the cache.
        tmc.invalidateAll();
    }

    // Create tables.
    public TableMetadataCache populateCache(String[] schemas,
            String[] tableNames) throws Exception
    {
        Table[] tables = new Table[schemas.length * tableNames.length];
        TableMetadataCache tmc = new TableMetadataCache(100);
        int tab = 0;

        for (String schema : schemas)
        {
            for (String tableName : tableNames)
            {
                Column historySeqno = new Column("seqno", Types.BIGINT);
                tables[tab] = new Table(schema, tableName);
                tables[tab].AddColumn(historySeqno);
                tmc.store(tables[tab]);
                tab++;
            }
        }

        return tmc;
    }
}
