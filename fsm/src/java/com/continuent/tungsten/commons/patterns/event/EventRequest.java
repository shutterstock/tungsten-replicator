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
 */

package com.continuent.tungsten.commons.patterns.event;

import java.util.concurrent.BlockingQueue;

import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * Defines an event request, which contains the event to be processed 
 * as well as an optional response queue to receive the result of 
 * processing. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventRequest
{
    private final Event event; 
    private final BlockingQueue<EventStatus> responseQueue;

    public EventRequest(Event event, BlockingQueue<EventStatus> responseQueue)
    {
        this.event = event;
        this.responseQueue = responseQueue; 
    }
    
    public Event getEvent()
    {
        return event;
    }
    
    public BlockingQueue<EventStatus> getResponseQueue()
    {
        return responseQueue;
    }
}
