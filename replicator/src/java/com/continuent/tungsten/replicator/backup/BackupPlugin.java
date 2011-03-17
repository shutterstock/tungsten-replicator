/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
package com.continuent.tungsten.replicator.backup;

/**
 * 
 * This class defines a BackupPlugin.  BackupPlugin instances are lightweight
 * objects created for a single backup operation.  Here is the lifecycle: 
 * 
 * <li>Instantiate plug-in from class name</li>
 * <li>Call setters on plug-in instance and load property names</li>
 * <li>Call configure() to signal configuration is complete</li>
 * <li>Call backup operation</li>
 * <li>Call release() to free resources</li>
 * </ol>
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface BackupPlugin
{
    /**
     * Complete plug-in configuration.  This is called after setters are 
     * invoked. 
     * 
     * @throws ReplicatorException Thrown if configuration is incomplete
     * or fails
     */
    public void configure() throws BackupException;

    /**
     * 
     * Release all resources used by plug-in.  This is called before the
     * plug-in is deallocated.  
     * 
     * @throws ReplicatorException Thrown if resource deallocation fails
     */
    public void release() throws BackupException;
}