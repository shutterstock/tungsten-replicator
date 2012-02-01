/**
 * Tungsten Scale-Out Stack
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.util.HashMap;
import java.util.Map;

/**
 * This class defines a ProtocolHandshake, which is sent from the master to a
 * slave as the slave connects. Masters offer capabilities to slaves.
 */
public class ProtocolHandshake extends ProtocolMessage
{
    static final long           serialVersionUID = 234524352L;

    private Map<String, String> capabilities     = new HashMap<String, String>();

    /**
     * Create a new instance.
     */
    public ProtocolHandshake()
    {
        super(null);
    }

    /** Sets a capability to a particular value. */
    public void setCapability(String name, String value)
    {
        getCapabilities().put(name, value);
    }

    /** Gets a capability value. */
    public String getCapability(String name)
    {
        return getCapabilities().get(name);
    }

    /**
     * Returns the current capability settings or null if no capabilities exist.
     * Older replicators do not return capabilities.
     */
    public Map<String, String> getCapabilities()
    {
        // Required for compatibility with older classes.
        if (capabilities == null)
            capabilities = new HashMap<String, String>();
        return capabilities;
    }
}