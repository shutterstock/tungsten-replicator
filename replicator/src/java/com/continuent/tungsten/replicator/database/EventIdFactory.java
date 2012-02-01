/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2012 Continuent Inc.
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

package com.continuent.tungsten.replicator.database;


/**
 * Factory to generate event IDs.
 */
public class EventIdFactory
{
    // Uses singleton design pattern.
    private static EventIdFactory instance = new EventIdFactory();

    private EventIdFactory()
    {
    }

    /**
     * Return factory instance.
     */
    public static EventIdFactory getInstance()
    {
        return instance;
    }

    /**
     * Return proper instance for a raw event ID or null if type cannot be
     * discovered.
     */
    public EventId createEventId(String rawEventId)
    {
        if (rawEventId.startsWith("mysql") || rawEventId.indexOf(":") > -1)
            return new MySQLEventId(rawEventId);
        else
            return null;
    }
}
