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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator;

import com.continuent.tungsten.commons.patterns.event.OutOfBandEvent;
import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * This class defines a ErrorNotification, which denotes a severe replication
 * error that causes replication to fail. It implements the OutOfBandEvent
 * interface to ensure it is processed out-of-band no matter how it is submitted
 * to the state machine.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ErrorNotification extends Event implements OutOfBandEvent
{
    private final String userMessage;
    private final long   seqno;
    private final String eventId;

    /**
     * Create new instance with underlying error and message for presentation to
     * users.
     */
    public ErrorNotification(String userMessage, Throwable e)
    {
        super(e);
        this.userMessage = userMessage;
        this.seqno = -1;
        this.eventId = null;
    }

    /**
     * Creates an error notification with user, a message, and replication
     * position information.
     */
    public ErrorNotification(String userMessage, long seqno, String eventId,
            Throwable e)
    {
        super(e);
        this.userMessage = userMessage;
        this.seqno = seqno;
        this.eventId = eventId;
    }

    /**
     * Returns the original source of the error.
     */
    public Throwable getThrowable()
    {
        return (Throwable) getData();
    }

    /**
     * Returns a message suitable for users.
     */
    public String getUserMessage()
    {
        return userMessage;
    }

    /**
     * Returns the log sequence number associated with failure or -1 if there is
     * no such number.
     */
    public long getSeqno()
    {
        return seqno;
    }

    /**
     * Returns the native event ID associated with failure or null if there is
     * no such ID.
     */
    public String getEventId()
    {
        return eventId;
    }
}