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

package com.continuent.tungsten.replicator.dbms;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests behavior of StatementData class to ensure accessors function properly
 * with both binary as well as string statements.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestStatementData
{
    /**
     * Confirm we can create statement data instance and get the query back.
     */
    @Test
    public void testStatementData() throws Exception
    {
        StatementData sd = new StatementData("foo");
        Assert.assertEquals("Checking value", "foo", sd.getQuery());
        sd.appendToQuery("bar");
        Assert.assertEquals("Checking appended value", "foobar", sd.getQuery());
    }

    /**
     * Confirm we can store query as binary data and get it back.
     */
    @Test
    public void testBinaryQuery() throws Exception
    {
        // Test values are Cafe' Be'be' where ' is an accent grave.
        String value1 = "Caf\u00E9";
        byte[] value1Bytes = value1.getBytes("ISO8859_1");
        String value2 = "B\u00E9b\u00E9";
        String both = value1 + value2;
        byte[] bothBytes = both.getBytes("ISO8859_1");

        // Confirm we can set and get the binary array value back.
        StatementData sd = new StatementData(null);
        sd.setCharset("ISO8859_1");
        sd.setQuery(value1Bytes);
        Assert.assertArrayEquals("Value matches in bytes", value1Bytes, sd
                .getQueryAsBytes());
        Assert.assertEquals("String value matches", value1, sd.getQuery());

        // Confirm that we can append a value and also get that back.
        sd.appendToQuery(value2);
        Assert.assertArrayEquals("Value matches in bytes", bothBytes, sd
                .getQueryAsBytes());
        Assert.assertEquals("String value matches", both, sd.getQuery());
    }
}