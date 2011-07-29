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

package com.continuent.tungsten.replicator.extractor;


/**
 * Denotes an extractor that extends normal Extractor capabilities to allow
 * parallel operation.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ParallelExtractor extends Extractor
{
    /**
     * Sets the ID of the task using this extractor. This method is called prior
     * to invoking the configure() method.
     * 
     * @param id Task ID
     * @see #configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void setTaskId(int id);

    /**
     * Returns the store name on which this extractor operates. This is used to
     * implement orderly shutdown and synchronize waits.
     */
    public String getStoreName();
}