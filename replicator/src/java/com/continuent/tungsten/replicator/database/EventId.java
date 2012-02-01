/**
 * Tungsten Scale-Out Stack
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
 * Denotes a native event ID, which is the ID used to identify [re-]start
 * locations in the DBMS log when extracting events.
 */
public interface EventId extends Comparable<EventId>
{
    /**
     * Return the event ID DBMS type.
     */
    public String getDbmsType();

    /** 
     * Returns true if this is a syntactically valid event ID.
     */
    public boolean isValid();

    /**
     * Compares two event IDs using the file index and offset as determinants
     * for collation. If the DBMS types are not the same or the eventID is invalid
     * the comparison result is undefined.
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(MySQLEventId eventId);

    /**
     * Prints event ID in standard format for this DBMS type.
     * 
     * @see java.lang.Object#toString()
     */
    public String toString();
}