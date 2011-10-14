/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.storage;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * Denotes a storage component that holds replication events.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface Store extends ReplicatorPlugin
{
    /** Gets the storage name. */
    public String getName();

    /** Sets the storage name. */
    public void setName(String name);

    /**
     * Returns the maximum persistently stored sequence number.
     */
    public long getMaxStoredSeqno();

    /**
     * Returns the minimum persistently stored sequence number.
     */
    public long getMinStoredSeqno();

    /**
     * Returns status information as a set of named properties.
     */
    public TungstenProperties status();
}