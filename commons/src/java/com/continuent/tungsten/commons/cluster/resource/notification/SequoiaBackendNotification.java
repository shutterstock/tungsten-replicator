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

import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Additional status values dedicated to sequoia backends
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class SequoiaBackendNotification extends ClusterResourceNotification
{
    /**
     * 
     */
    private static final long serialVersionUID = -3506410692814268800L;

    /** Controller to which this backend belongs */

    public SequoiaBackendNotification(String clusterName, String memberName,
            String resourceName, String state, String source,
            TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, null, memberName, null, ResourceType.ANY, null, null, null);
        // public ReplicatorNotification(String clusterName, String
        // clusterMemberName, String resourceName,
        // String state, String source, TungstenProperties resourceProps)
        // TODO: host and controller probably overlap.
        // super(ResourceTypes., name, state, host, role, 99, service, url,
        // driver, null);
        // TungstenProperties additionalProperties = new TungstenProperties();
        // additionalProperties.setString(PROPERTY_KEY_CONTROLLER, controller);
        // setProperties(additionalProperties.map());

    }
}
