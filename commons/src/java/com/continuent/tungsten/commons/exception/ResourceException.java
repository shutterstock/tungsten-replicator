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
 * Initial developer(s): Edward Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.exception;

public class ResourceException extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID = 6387458316241931596L;

    public ResourceException()
    {
        // TODO Auto-generated constructor stub
    }

    public ResourceException(String message)
    {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public ResourceException(Throwable cause)
    {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    public ResourceException(String message, Throwable cause)
    {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

}
