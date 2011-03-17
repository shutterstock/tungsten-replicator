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
 * Initial developer(s): Edward Archibald
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.commons.cluster.resource.notification;

import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class ClusterConfigurationNotification
        extends ClusterResourceNotification
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param clusterName
     * @param memberName TODO
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param resourceProps
     */
    public ClusterConfigurationNotification(String clusterName,
            String memberName, String notificationSource,
            String resourceName, ResourceState resourceState, TungstenProperties resourceProps)
    {
        super(NotificationStreamID.MONITORING, clusterName,
                memberName, notificationSource,
                ResourceType.CONFIGURATION, resourceName, resourceState, resourceProps);
    }
}
