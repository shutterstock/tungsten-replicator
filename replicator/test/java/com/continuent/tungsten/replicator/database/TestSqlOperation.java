/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import org.junit.Assert;
import org.junit.Test;

/**
 * Implements simple unit tests on SqlOperation class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestSqlOperation
{
    /** Verify default constructor settings. */
    @Test
    public void testDefaultConstructor() throws Exception
    {
        SqlOperation op = new SqlOperation();
        Assert.assertNull(op.getName());
        Assert.assertNull(op.getSchema());
        Assert.assertEquals(op.getObjectType(), SqlOperation.UNRECOGNIZED);
        Assert.assertEquals(op.getOperation(), SqlOperation.UNRECOGNIZED);
    }

    /** Verify constructor with explicit metadata values. */
    @Test
    public void testExplicitConstructor() throws Exception
    {
        SqlOperation op = new SqlOperation(SqlOperation.TABLE,
                SqlOperation.CREATE, "foo", "bar");
        Assert.assertEquals("foo", op.getSchema());
        Assert.assertEquals("bar", op.getName());
        Assert.assertEquals(op.getObjectType(), SqlOperation.TABLE);
        Assert.assertEquals(op.getOperation(), SqlOperation.CREATE);
    }

    /** Verify assignment of qualified name with and without schema. */
    @Test
    public void testQname() throws Exception
    {
        SqlOperation op = new SqlOperation(SqlOperation.TABLE,
                SqlOperation.CREATE, "x", "y");

        op.setQualifiedName("foo.bar");
        Assert.assertEquals("foo", op.getSchema());
        Assert.assertEquals("bar", op.getName());
        
        op.setQualifiedName("foo");
        Assert.assertNull(op.getSchema());
        Assert.assertEquals("foo", op.getName());
    }
}