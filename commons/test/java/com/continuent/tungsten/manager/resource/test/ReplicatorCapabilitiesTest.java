/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.manager.resource.test;

import junit.framework.TestCase;

import com.continuent.tungsten.commons.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Implements a simple unit test for Tungsten properties.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ReplicatorCapabilitiesTest extends TestCase
{
    /**
     * Tests round trip storage in properties.
     */
    public void testProperties() throws Exception
    {
        // Check empty instance.
        ReplicatorCapabilities caps1 = new ReplicatorCapabilities();
        TungstenProperties cprops = caps1.asProperties();
        ReplicatorCapabilities caps2 = new ReplicatorCapabilities(cprops);
        testEquality(caps1, caps2);

        // Check instance with values. 
        caps1 = new ReplicatorCapabilities();
        caps1.addRole("master");
        caps1.addRole("slave");
        caps1.setConsistencyCheck(true);
        caps1.setFlush(true);
        caps1.setHeartbeat(true);
        caps1.setProvisionDriver(ReplicatorCapabilities.PROVISION_DONOR);
        caps1.setModel(ReplicatorCapabilities.MODEL_PEER);
        cprops = caps1.asProperties();
        caps2 = new ReplicatorCapabilities(cprops);
        testEquality(caps1, caps2);
    }

    // Test that two capabilities instances are equal. 
    private void testEquality(ReplicatorCapabilities rc1,
            ReplicatorCapabilities rc2)
    {
        assertEquals("model", rc1.getModel(), rc2.getModel());
        assertEquals("driver", rc1.getProvisionDriver(), rc2.getProvisionDriver());
        assertEquals("roles", rc1.getRoles().size(), rc2.getRoles().size());
        assertEquals("consistency check", rc1.isConsistencyCheck(), rc2.isConsistencyCheck());
        assertEquals("flush", rc1.isFlush(), rc2.isFlush());
        assertEquals("heartbeat", rc1.isHeartbeat(), rc2.isHeartbeat());
    }
}
