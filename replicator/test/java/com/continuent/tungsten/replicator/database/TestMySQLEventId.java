/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2012 Continuent Inc.
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

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests behavior of MySQL event ID -- parsing and comparison.
 */
public class TestMySQLEventId
{
    /**
     * Verify that valid event IDs parse correctly.
     */
    @Test
    public void testValidEventIds() throws Exception
    {
        EventIdFactory factory = EventIdFactory.getInstance();

        // Test a standard value.
        MySQLEventId e1 = (MySQLEventId) factory
                .createEventId("mysql-bin.014371:0000000064207416;1");
        Assert.assertNotNull(e1);
        Assert.assertEquals("mysql-bin.014371", e1.getFileName());
        Assert.assertEquals(14371, e1.getFileIndex());
        Assert.assertEquals(64207416, e1.getOffset());
        Assert.assertEquals(1, e1.getSessionId());
        Assert.assertTrue(e1.isValid());
        Assert.assertEquals("mysql-bin.014371:0000000064207416;1",
                e1.toString());

        // Test a shorter one with no session ID.
        MySQLEventId e2 = (MySQLEventId) factory
                .createEventId("mysql-bin.014371:64207416");
        Assert.assertNotNull(e2);
        Assert.assertEquals("mysql-bin.014371", e2.getFileName());
        Assert.assertEquals(14371, e2.getFileIndex());
        Assert.assertEquals(64207416, e2.getOffset());
        Assert.assertEquals(-1, e2.getSessionId());
        Assert.assertTrue(e2.isValid());
        Assert.assertEquals("mysql-bin.014371:0000000064207416", e2.toString());

        // Test a shorter one with no session ID and no file prefix. This form
        // is used for
        // Tungsten 1.x replicators.
        MySQLEventId e3 = (MySQLEventId) factory
                .createEventId("014371:64207416;235");
        Assert.assertNotNull(e3);
        Assert.assertEquals("014371", e3.getFileName());
        Assert.assertEquals(14371, e3.getFileIndex());
        Assert.assertEquals(64207416, e3.getOffset());
        Assert.assertEquals(235, e3.getSessionId());
        Assert.assertTrue(e3.isValid());
        Assert.assertEquals("014371:0000000064207416;235", e3.toString());
    }

    /**
     * Verify that invalid event IDs do not parse correctly. Do not use the
     * factory as it may not even return the correct event ID type.
     */
    @Test
    public void testInvalidEventIds() throws Exception
    {
        String[] badEventIds = {"", "mysql-bin", "014371",
                "mysql-bin.014371;0000000064207416;1", "014371",
                "mysql-sql014371:64207416;1", "mysql-sql014371:64207416A;1"};
        for (String badEventId : badEventIds)
        {
            MySQLEventId eventId = new MySQLEventId(badEventId);
            Assert.assertFalse(badEventId, eventId.isValid());
        }
    }

    /**
     * Verify that events IDs compare using the file index and offset values.
     */
    @Test
    public void testCompareEventIds() throws Exception
    {
        // Test equality.
        compareEventIds("mysql-bin.014371:0000000064207416;1",
                "014371:64207416", 0);
        compareEventIds("014371:64207416;253",
                "mysql-bin.014371:0000000064207416;1", 0);
        compareEventIds("mysql-bin.014371:0000000064207416;1",
                "mysql-bin.014371:0000000064207416;9999935", 0);

        // Test less than.
        compareEventIds("mysql-bin.014370:0000000064207416;1",
                "mysql-bin.014371:0000000064207416;1", -1);
        compareEventIds("mysql-bin.014371:0000000064207415;1",
                "mysql-bin.014371:0000000064207416;1", -1);
        compareEventIds("mysql-bin.014371:0000000064207415",
                "014371:0000000064207416", -1);

        // Test greater than.
        compareEventIds("mysql-bin.014372:0000000064207416;1",
                "mysql-bin.014371:0000000064207415;1", 1);
        compareEventIds("mysql-bin.014371:0000000064207417;1",
                "mysql-bin.014371:0000000064207416;1", 1);
        compareEventIds("014371:64207417",
                "mysql-bin.014371:0000000064207416;1", 1);
    }

    /**
     * Generate event ID instances, compare, and check result.
     */
    private void compareEventIds(String e1, String e2, int expectedOutcome)
            throws Exception
    {
        EventIdFactory factory = EventIdFactory.getInstance();
        EventId eid1 = factory.createEventId(e1);
        EventId eid2 = factory.createEventId(e2);
        int outcome = eid1.compareTo(eid2);

        Assert.assertEquals("Comparing: e1=" + e1 + " e2=" + e2,
                expectedOutcome, outcome);

    }
}