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

package com.continuent.tungsten.replicator;

/**
 * This class defines a ReplicatorException, a parent for all other exceptions
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ReplicatorException extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID     = -2849591301389282829L;

    private String            originalErrorMessage = null;
    private String            extraData            = null;

    /**
     * Creates a new <code>ReplicatorException</code> object
     */
    public ReplicatorException()
    {
        super();
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     */
    public ReplicatorException(String arg0)
    {
        super(arg0);
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     */
    public ReplicatorException(Throwable arg0)
    {
        super(arg0);
        if (arg0 instanceof ReplicatorException)
        {
            ReplicatorException exc = (ReplicatorException) arg0;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        }
    }

    /**
     * Creates a new <code>ReplicatorException</code> object
     * 
     * @param arg0
     * @param arg1
     */
    public ReplicatorException(String arg0, Throwable arg1)
    {
        super(arg0, arg1);
        if (arg1 instanceof ReplicatorException)
        {
            ReplicatorException exc = (ReplicatorException) arg1;
            this.extraData = exc.extraData;
            this.originalErrorMessage = exc.originalErrorMessage;
        }
        else 
            this.originalErrorMessage = arg0;
    }

    public void setOriginalErrorMessage(String originalErrorMessage)
    {
        this.originalErrorMessage = originalErrorMessage;
    }
    

    public String getOriginalErrorMessage()
    {
        return originalErrorMessage;
    }

    public String getExtraData()
    {
        return extraData;
    }

    public void setExtraData(String extraData)
    {
        this.extraData = extraData;
    }
}
