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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.consistency;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ConsistencyException
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alexey Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyException extends ReplicatorException
{

    /**
     * 
     */
    private static final long serialVersionUID = 6105152751419283356L;

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     */
    public ConsistencyException()
    {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     */
    public ConsistencyException(String arg0)
    {
        super(arg0);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     */
    public ConsistencyException(Throwable arg0)
    {
        super(arg0);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ConsistencyException</code> object
     * 
     * @param arg0
     * @param arg1
     */
    public ConsistencyException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
        // TODO Auto-generated constructor stub
    }

}
