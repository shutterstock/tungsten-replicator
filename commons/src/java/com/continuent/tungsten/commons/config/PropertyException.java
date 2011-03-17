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

package com.continuent.tungsten.commons.config;

/**
 * Represents an error that occurs during property processing such as a datatype
 * violation or a missing required value.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class PropertyException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with a message.
     */
    public PropertyException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new exception with a message and underlying exception.
     */
    public PropertyException(String msg, Throwable t)
    {
        super(msg, t);
    }
}