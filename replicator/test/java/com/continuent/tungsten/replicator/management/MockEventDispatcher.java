/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

package com.continuent.tungsten.replicator.management;

import com.continuent.tungsten.commons.patterns.event.EventCompletionListener;
import com.continuent.tungsten.commons.patterns.event.EventDispatcher;
import com.continuent.tungsten.commons.patterns.event.EventRequest;
import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * Dummy event dispatcher used for testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MockEventDispatcher implements EventDispatcher
{
    @Override
    public void setListener(EventCompletionListener listener)
    {
        // Do nothing.
    }

    @Override
    public EventRequest put(Event event) throws InterruptedException
    {
        // Do nothing.
        return null;
    }

    @Override
    public EventRequest putOutOfBand(Event event) throws InterruptedException
    {
        // Do nothing.
        return null;
    }

    @Override
    public boolean cancelActive(EventRequest request,
            boolean mayInterruptIfRunning) throws InterruptedException
    {
        // Do nothing.
        return false;
    }
}
