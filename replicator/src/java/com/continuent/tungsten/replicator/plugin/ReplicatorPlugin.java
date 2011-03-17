/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.plugin;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ReplicatorPlugin. Replicator plug-ins have the following
 * life cycle:
 * <p>
 * <ol>
 * <li>Instantiate plug-in from class name</li>
 * <li>Call setters on plug-in instance and load property names</li>
 * <li>Call configure() to signal configuration is complete</li>
 * <li>Call prepare() to create resources for operation</li>
 * <li>(Type-specific plug-in method calls)</li>
 * <li>Call release() to free resources</li>
 * </ol>
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public interface ReplicatorPlugin
{
    /**
     * Complete plug-in configuration. This is called after setters are invoked
     * at the time that the replicator goes through configuration.
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete or
     *             fails
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException;

    /**
     * Prepare plug-in for use. This method is assumed to allocate all required
     * resources. It is called before the plug-in performs any operations.
     * 
     * @throws ReplicatorException Thrown if resource allocation fails
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException;

    /**
     * Release all resources used by plug-in. This is called before the plug-in
     * is deallocated.
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException;
}
