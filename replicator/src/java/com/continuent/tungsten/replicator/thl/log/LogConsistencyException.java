/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 *
 */
package com.continuent.tungsten.replicator.thl.log;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Denotes an exception due to a consistency problem in the log. 
 */
public class LogConsistencyException extends THLException
{
    private static final long serialVersionUID = 1L;

    public LogConsistencyException(String msg)
    {
        super(msg);
    }

    public LogConsistencyException(String msg, Exception e)
    {
        super(msg, e);
    }
}
