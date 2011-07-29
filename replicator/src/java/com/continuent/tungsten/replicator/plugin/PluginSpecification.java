/**
 * Tungsten Scale-Out Stack
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

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.filter.FilterManualProperties;

/**
 * Specification for a component, including the implementation class and input
 * properties, and utility methods to manage the component life cycle.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PluginSpecification
{
    private final String             prefix;
    private final String             name;
    private final Class<?>           pluginClass;
    private final TungstenProperties properties;

    public PluginSpecification(String prefix, String name,
            Class<?> pluginClass, TungstenProperties properties)
    {
        this.prefix = prefix;
        this.name = name;
        this.pluginClass = pluginClass;
        this.properties = properties;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public String getName()
    {
        return name;
    }

    public Class<?> getPluginClass()
    {
        return pluginClass;
    }

    public TungstenProperties getProperties()
    {
        return properties;
    }

    /**
     * Instantiate the plugin and assign properties. 
     * 
     * @throws PluginException Thrown if instantiation fails
     */
    public ReplicatorPlugin instantiate(int id) throws ReplicatorException
    {
        ReplicatorPlugin plugin = PluginLoader.load(pluginClass.getName());
        if (plugin instanceof FilterManualProperties)
            ((FilterManualProperties) plugin).setConfigPrefix(prefix);
        else
            properties.applyProperties(plugin);
        return plugin;
    }
}
