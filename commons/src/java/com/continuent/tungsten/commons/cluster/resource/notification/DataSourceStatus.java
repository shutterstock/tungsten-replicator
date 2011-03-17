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

import com.continuent.tungsten.commons.cluster.resource.logical.DataShardFacetRole;
import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Provides utilities for resource status specific properties, specialized in
 * data source representation
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class DataSourceStatus extends ResourceStatus {
	public static final String PROPERTY_KEY_SERVICE = "dataServiceName";
	public static final String PROPERTY_KEY_HOST = "host";
	public static final String PROPERTY_KEY_ROLE = "role";
	public static final String PROPERTY_KEY_PRECEDENCE = "precedence";
	public static final String PROPERTY_KEY_URL = "url";
	public static final String PROPERTY_KEY_DRIVER = "driver";
	public static final String PROPERTY_KEY_VENDOR = "vendor";

	/**
	 * Inner class enumerating the different possible RouterResource Roles<br>
	 * Roles are defined as strings so that they can be sent with native java
	 * functions by the notifier
	 * 
	 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
	 * @version 1.0
	 */
	public static final class Roles {
		public static final String SLAVE = DataShardFacetRole.slave.toString();
		public static final String MASTER = DataShardFacetRole.master.toString();
		public static final String UNKNOWN = DataShardFacetRole.undefined.toString();
	}

	/**
	 * Creates a new <code>DataSourceStatus</code> object filling in additional
	 * role, service, url and driver information
	 * 
	 * @param type
	 *            Type of RouterResource
	 * @param name
	 *            Possibly unique identifier for this resource
	 * @param state
	 *            Last known state
	 * @param host
	 *            hostname on which this data source runs
	 * @param role
	 *            slave or master
	 * @param precedence
	 * 			  used for failover
	 * @param service
	 *            service to which this datasource belongs
	 * @param url
	 *            url string used to connect to this datasource
	 * @param driver
	 *            driver as a string used for connection to this datasource
	 * @param vendor
	 * 			  vendor of the driver
	 */
	public DataSourceStatus(String type, String name, String state,
			String host, String role, int precedence, String service, String url, String driver, String vendor) {
		super(type, name, state, null);
		TungstenProperties datasourceProperties = new TungstenProperties();
		datasourceProperties.setString(PROPERTY_KEY_SERVICE, service);
		datasourceProperties.setString(PROPERTY_KEY_HOST, host);
		datasourceProperties.setString(PROPERTY_KEY_ROLE, role);
		datasourceProperties.setInt(PROPERTY_KEY_PRECEDENCE, precedence);
		datasourceProperties.setString(PROPERTY_KEY_URL, url);
		datasourceProperties.setString(PROPERTY_KEY_DRIVER, driver);
		datasourceProperties.setString(PROPERTY_KEY_VENDOR, vendor);
		setProperties(datasourceProperties.map());
	}
}
