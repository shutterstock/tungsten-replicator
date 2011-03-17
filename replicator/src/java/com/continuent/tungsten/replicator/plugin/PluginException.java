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

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * 
 * This class defines a PluginException
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class PluginException extends ReplicatorException
{

    private static final long serialVersionUID = 1L;

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     */
    public PluginException(String msg)
    {
        super(msg);
    }

    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param throwable
     */
    public PluginException(Throwable throwable)
    {
        super(throwable);
    }
    
    /**
     * 
     * Creates a new <code>PluginException</code> object
     * 
     * @param msg
     * @param throwable
     */
    public PluginException(String msg, Throwable throwable)
    {
        super(msg, throwable);
    }
    
}
