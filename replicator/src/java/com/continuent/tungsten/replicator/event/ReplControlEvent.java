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
 */

package com.continuent.tungsten.replicator.event;


/**
 * An implementation of ReplEvent used to transmit control information within
 * pipelines. Control events add extra information that affects the disposition
 * of processing following a particular event. They are not serialized and
 * should never be handled by an applier.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
@SuppressWarnings("serial")
public class ReplControlEvent extends ReplEvent
{
    // Control event types.
    /**
     * Event indicates end of processing. Task should immediately commit current
     * work and exit.
     */
    public static final int               STOP                 = 1;

    /**
     * Event is provided for synchronization purposes when waiting for a
     * particular sequence number or event. Synchronization events ensure that
     * all tasks "see" an event on which we are waiting when parallel apply is
     * active.
     */
    public static final int               SYNC = 2;

    // Control event data.
    private int                           eventType;
    private ReplDBMSEvent                 event;

    /**
     * Creates a new control event instance.
     * 
     * @param eventType A static control event type
     */
    public ReplControlEvent(int eventType)
    {
        this.eventType = eventType;
    }

    /** Returns the control event type. */
    public int getEventType()
    {
        return eventType;
    }

    /**
     * Returns the event to which control information applies or null if
     * inapplicable.
     */
    public ReplDBMSEvent getEvent()
    {
        return event;
    }

    public void setEvent(ReplDBMSEvent event)
    {
        this.event = event;
    }
}