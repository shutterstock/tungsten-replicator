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
 * Initial developer(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.filter;

/**
 * This class defines a more raw Filter by the fact that its properties are not
 * automatically set by using setter methods. Implementations must configure
 * their properties manually.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public interface FilterManualProperties extends Filter
{
    /**
     * Set filter's configuration prefix. This is important in order for the
     * filter to be able to know where its properties are in the configuration
     * file.<br/>
     * Eg. of how filter's properties could be read:<br/>
     * <code>
     * TungstenProperties filterProperties = properties.subset(configPrefix
                + ".", true);
     * </code>
     * 
     * @param configPrefix Configuration prefix.
     * @see com.continuent.tungsten.commons.config.TungstenProperties#subset(String, boolean)
     */
    public void setConfigPrefix(String configPrefix);
}
