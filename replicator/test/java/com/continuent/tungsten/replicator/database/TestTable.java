/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.database;

import junit.framework.Assert;

import org.junit.Test;

import com.continuent.tungsten.replicator.consistency.ConsistencyTable;

public class TestTable
{

    @Test
    public void testClone()
    {
        Table original = ConsistencyTable.getConsistencyTableDefinition("test");
        Table clone = original.clone();

        int originalKeyCount = original.getKeys().size();

        // Remove PK from the cloned table.
        Key primaryKey = clone.getPrimaryKey();
        Assert.assertNotNull(
                "Consistency table has a PK (did declaration changed?)",
                primaryKey);
        Assert.assertTrue("PK is successfully removed",
                clone.getKeys().remove(primaryKey));
        Assert.assertFalse(
                "Clone table has no PK in key array after removing it", clone
                        .getKeys().contains(primaryKey));

        // Removed PK should still be there in the original table.
        Key originalPrimaryKey = original.getPrimaryKey();
        Assert.assertNotNull(
                "Original table contains PK after PK was removed from the clone",
                originalPrimaryKey);
        Assert.assertTrue("Original table key array still contains PK",
                original.getKeys().contains(originalPrimaryKey));
        Assert.assertEquals(
                "Original table key count didn't change after removing PK from the clone",
                originalKeyCount, original.getKeys().size());
    }

}
