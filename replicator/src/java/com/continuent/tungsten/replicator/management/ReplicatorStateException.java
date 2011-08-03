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
 * Initial developer(s): Robert Hodges
 */
package com.continuent.tungsten.replicator.management;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Defines a non-fatal exception that occurred during replicator state machine
 * processing. 
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka Kurikka</a>
 * @version 1.0
 */
public class ReplicatorStateException extends ReplicatorException
{
    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new <code>ReplicatorStateException</code> object
     * 
     * @param message Message suitable for display to clients
     */
    public ReplicatorStateException(String message)
    {
        super(message);
    }

    public ReplicatorStateException(String message, Throwable e)
    {
        super(message, e);
    }
}