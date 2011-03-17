/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Scott Martin
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.database;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * 
 * This class defines a DatabaseException
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class DatabaseException extends ReplicatorException
{
    static final long serialVersionUID = 1L;
   
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     *
     */
    public DatabaseException()
    {
        super();
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param msg
     */
    public DatabaseException(String msg)
    {
        super(msg);
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param msg
     * @param cause
     */
    public DatabaseException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
    
    /**
     * 
     * Creates a new <code>DatabaseException</code> object
     * 
     * @param cause
     */
    public DatabaseException(Throwable cause)
    {
        super(cause);
    }
    
}
