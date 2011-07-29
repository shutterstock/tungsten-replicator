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

package com.continuent.tungsten.replicator.plugin;

import java.lang.reflect.Method;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a PluginConfigurator
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginConfigurator
{
    /**
     * 
     * Call setter method for given ReplicatorPlugin.
     * 
     * @param plugin ReplicatorPlugin instance
     * @param name The name of the setter method to be called
     * @param value Argument to be passed for setter method
     * @throws ReplicatorException
     */
    static public void setParameter(ReplicatorPlugin plugin, String name,
            Object value) throws ReplicatorException
    {
        Method[] methods = plugin.getClass().getMethods();
        for (Method m : methods)
        {
            if (m.getName().equals(name) == false)
                continue;
            Class<?>[] types = m.getParameterTypes();
            if (types.length != 1)
                throw new PluginException("Method " + name + " for class "
                        + value.getClass() + " not found");
            try
            {
                m.invoke(plugin, value);
            }
            catch (Exception e)
            {
                throw new PluginException("Error in method invocation", e);
            }
            return;
        }
        throw new PluginException("Method " + name + " not found");
    }

    /**
     * 
     * Call getter method for given replicator plugin.
     * 
     * @param plugin ReplicatorPlugin instance
     * @param name The name of the getter method to be called
     * @return Return value of getter method
     * @throws ReplicatorException
     */
    static public Object getParameter(ReplicatorPlugin plugin, String name)
            throws ReplicatorException
    {
        Method[] methods = plugin.getClass().getMethods();
        for (Method m : methods)
        {
            if (m.getName().equals(name) == false)
                continue;
            Class<?>[] types = m.getParameterTypes();
            if (types.length != 0)
                throw new PluginException("Method " + name + " not found");
            try {
                return m.invoke(plugin);
            }
            catch (Exception e)
            {
                throw new PluginException("Error in method invocation", e);
            }
        }
        throw new PluginException("Method " + name + " not found");
    }
}
