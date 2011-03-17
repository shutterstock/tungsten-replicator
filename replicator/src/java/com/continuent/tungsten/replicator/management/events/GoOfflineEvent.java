/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges
 */
package com.continuent.tungsten.replicator.management.events;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * Signals that the replicator should move to the off-line state. This event may
 * be submitted by underlying code to initiate a controlled shutdown.
 */
public class GoOfflineEvent extends Event
{
    private TungstenProperties params;

    public GoOfflineEvent()
    {
        this(new TungstenProperties());
    }

    public GoOfflineEvent(TungstenProperties params)
    {
        super(null);
        this.params = params;
    }

    public TungstenProperties getParams()
    {
        return params;
    }
}
