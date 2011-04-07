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
 * Initial developer(s): Robert Hodges and Csaba Simon.
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class defines a ExtractorException
 *
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ExtractorException extends ReplicatorException
{
    static final long    serialVersionUID = 1L;
    private final String eventId;

    /**
     * Creates a new exception with only a message.
     *
     * @param msg
     */
    public ExtractorException(String msg)
    {
        this(msg, null, null);
    }

    /**
     * Creates a new exception with only a cause but no message.
     *
     * @param t exception to link cause to
     */
    public ExtractorException(Throwable t)
    {
        this(null, t, null);
    }

    /**
     * Creates a new exception with message and cause,
     *
     * @param msg
     * @param cause
     */
    public ExtractorException(String msg, Throwable cause)
    {
        this(msg, cause, null);
    }

    /**
     * Creates a new exception with message, cause, and associated native
     * eventId.
     */
    public ExtractorException(String msg, Throwable cause, String eventId)
    {
        super(msg, cause);
        this.eventId = eventId;
    }

    public String getEventId()
    {
        return eventId;
    }
}
