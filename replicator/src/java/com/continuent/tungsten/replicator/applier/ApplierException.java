/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ApplierException
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ApplierException extends ReplicatorException
{

    /**
     * 
     */
    private static final long serialVersionUID = 8214495751377199032L;

    /**
     * Creates a new <code>ApplierException</code> object
     */
    public ApplierException()
    {
    }

    /**
     * Creates a new <code>ApplierException</code> object
     * 
     * @param message
     */
    public ApplierException(String message)
    {
        super(message);
    }

    /**
     * Creates a new <code>ApplierException</code> object
     * 
     * @param cause
     */
    public ApplierException(Throwable cause)
    {
        super(cause);
    }

    /**
     * Creates a new <code>ApplierException</code> object
     * 
     * @param message
     * @param cause
     */
    public ApplierException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
