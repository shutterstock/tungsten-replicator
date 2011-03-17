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
 * Initial developer(s): Ed Archibald
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.commons.cluster.resource.shared;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class ResourceConfiguration extends Resource
{

    private TungstenProperties properties       = new TungstenProperties();
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;

    public ResourceConfiguration()
    {
        super(ResourceType.CONFIGURATION, "UNKNOWN");
        init();

    }

    public ResourceConfiguration(String name)
    {
        super(ResourceType.CONFIGURATION, name);
        init();
    }

    /**
     * Creates a new <code>ResourceConfiguration</code> object with underlying
     * TungstenProperties.
     */
    public ResourceConfiguration(String name, TungstenProperties properties)
    {
        super(ResourceType.CONFIGURATION, name);
        this.properties = properties;
        init();
    }

    /**
     * Returns TungstenProperties corresponding to this
     * <code>ResourceConfiguration</code>.
     */
    public TungstenProperties getProperties()
    {
        return properties;
    }

    private void init()
    {
        this.childType = ResourceType.ANY;
        this.isContainer = false;
    }

    public String describe(boolean detailed)
    {
        return TungstenProperties.formatProperties(
                getName() + " configuration", properties, "");
    }

}
