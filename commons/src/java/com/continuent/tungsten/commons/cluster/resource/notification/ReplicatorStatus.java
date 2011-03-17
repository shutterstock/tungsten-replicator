/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.commons.cluster.resource.notification;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Additional status values dedicated to replicator instances
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class ReplicatorStatus extends DataSourceStatus
{
    /**
     * Creates a new <code>ReplicatorStatus</code> object filling in
     * additional latency information
     * 
     * @param type Type of RouterResource
     * @param name Possibly unique identifier for this resource
     * @param state Last known state
     * @param host hostname on which this replicator is executing
     * @param role slave or master
     * @param precedence precedence to use for failover
     * @param service service to which this datasource belongs
     * @param url url string used to connect to this datasource
     * @param driver driver as a string used for connection to this datasource
     * @param vendor vendor for driver
     * @param status arbitrary set of metrics and properties provided by the
     *            replicator
     */
    public ReplicatorStatus(String type, String name, String state,
            String host, String role, int precedence, String service, String url,
            String driver, String vendor, TungstenProperties status)
    {
        super(type, name, state, host, role, precedence, service, url, driver, vendor);
        setProperties(status.map());
    }
}
