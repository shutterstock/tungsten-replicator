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
 * Contributor(s): Robert Hodges
 */
package com.continuent.tungsten.commons.patterns.notification;

/**
 * This class defines a ResourceNotificationException
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class ResourceNotificationException extends Exception
{
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     */
    public ResourceNotificationException()
    {
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param message
     */
    public ResourceNotificationException(String message)
    {
        super(message);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param cause
     */
    public ResourceNotificationException(Throwable cause)
    {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new <code>ResourceNotificationException</code> object
     * 
     * @param message
     * @param cause
     */
    public ResourceNotificationException(String message, Throwable cause)
    {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

}
