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
 * Contributor(s):
 */

package com.continuent.tungsten.commons.jmx;

/**
 * Denotes an unexpected error in server processing. The current operation
 * cannot continue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ServerRuntimeException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     */
    public ServerRuntimeException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new <code>ServerRuntimeException</code> object
     * 
     * @param msg Message describing the problem
     * @param cause Root cause of the exception
     */
    public ServerRuntimeException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
