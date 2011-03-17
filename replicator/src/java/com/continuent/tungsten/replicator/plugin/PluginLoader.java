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

/**
 * 
 * This class defines a PluginLoader
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginLoader
{
    /**
     * Load plugin implementation.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin
     * @throws PluginException
     */
    static public ReplicatorPlugin load(String name) throws PluginException
    {
        if (name == null)
            throw new PluginException("null");
        try
        {
            return (ReplicatorPlugin) Class.forName(name).newInstance();
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }

    /**
     * Load plugin class.
     * 
     * @param name The name of the plugin implementation class to be loaded.
     * @return new plugin class
     * @throws PluginException
     */
    static public Class<?> loadClass(String name) throws PluginException
    {
        if (name == null)
            throw new PluginException("null");
        try
        {
            return (Class<?>) Class.forName(name);
        }
        catch (Exception e)
        {
            throw new PluginException(e);
        }
    }
}
