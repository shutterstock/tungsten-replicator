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
 * Tests table matching rules.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestTableMatcher
{
    /**
     * Verify that an empty matcher does not match anything.
     */
    @Test
    public void testEmpty() throws Exception
    {
        TableMatcher tm = new TableMatcher();

        tm.prepare(null);
        Assert.assertFalse(tm.match(null, null));
        Assert.assertFalse(tm.match("test", "foo"));

        tm.prepare("");
        Assert.assertFalse(tm.match("test", "foo"));

        tm.prepare(",,,");
        Assert.assertFalse(tm.match("test", "foo"));
        Assert.assertFalse(tm.match("", ""));
    }

    /**
     * Verify that we match schema names without wild cards.
     */
    @Test
    public void testBasicSchemas() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test1,test2");

        Assert.assertTrue(tm.match("test1", null));
        Assert.assertTrue(tm.match("test2", "foo"));
        Assert.assertFalse(tm.match("test", ""));
        Assert.assertFalse(tm.match("test11", ""));
    }

    /**
     * Verify that we match schema names with wild cards.
     */
    @Test
    public void testWildSchemas() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test?,special*");

        Assert.assertTrue(tm.match("test1", null));
        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertFalse(tm.match("test12", ""));
        Assert.assertFalse(tm.match("xtest1", ""));
        Assert.assertFalse(tm.match("test", ""));

        Assert.assertTrue(tm.match("special", ""));
        Assert.assertTrue(tm.match("specialX", "foo"));
        Assert.assertFalse(tm.match("Sspecial", "foo"));
    }

    /**
     * Verify that we match table names without wild cards.
     */
    @Test
    public void testBasicTables() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test1.foo,test2.bar");

        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertTrue(tm.match("test2", "bar"));

        Assert.assertFalse(tm.match("test", "foo"));
        Assert.assertFalse(tm.match("test1", null));
        Assert.assertFalse(tm.match("test1", "bar"));
    }

    /**
     * Verify that we match table names with wild cards.
     */
    @Test
    public void testWildTables() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test?.foo,test?.b*r,special.fo?");

        Assert.assertTrue(tm.match("test1", "foo"));
        Assert.assertFalse(tm.match("test12", "foo"));
        Assert.assertFalse(tm.match("test1", "foo1"));

        Assert.assertTrue(tm.match("test1", "br"));
        Assert.assertTrue(tm.match("test2", "baaar"));
        Assert.assertFalse(tm.match("test3", "bara"));
        Assert.assertFalse(tm.match("test3", "bara"));

        Assert.assertTrue(tm.match("special", "foo"));
        Assert.assertTrue(tm.match("special", "for"));
        Assert.assertFalse(tm.match("special", "fo"));
        Assert.assertFalse(tm.match("special", "fooo"));
    }

    /**
     * Verify that we match mixed schema and table names.
     */
    @Test
    public void testMixed() throws Exception
    {
        TableMatcher tm = new TableMatcher();
        tm.prepare("test,test1.bar,*.foo");

        // These should all match.
        Assert.assertTrue(tm.match("test", "fix"));
        Assert.assertTrue(tm.match("test", "bar"));
        Assert.assertTrue(tm.match("test1", "bar"));
        Assert.assertTrue(tm.match("test25", "foo"));

        // These should not match.
        Assert.assertFalse(tm.match("test25", "bar"));
        Assert.assertFalse(tm.match("test1", "barx"));
        Assert.assertFalse(tm.match("db25", "xfoo"));
    }
}