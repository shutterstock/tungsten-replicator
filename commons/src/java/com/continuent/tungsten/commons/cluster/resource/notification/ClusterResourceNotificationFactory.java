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

import java.util.Map;

import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * The function of this class is to convert notifications from various
 * components in the system into notifications that can be more profitably used
 * in the complex event processing required by the cluster policy manager.
 * 
 * @author edward
 */
public class ClusterResourceNotificationFactory
{
    public static ClusterResourceNotification createInstance(
            String clusterName, String memberName, Map<String, Object> monitorNotification)
    {
        ResourceType resourceType = (ResourceType) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_TYPE);
        String notificationSource = (String) monitorNotification
                .get(NotificationPropertyKey.KEY_NOTIFICATION_SOURCE);
        ResourceState resourceState = (ResourceState) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_STATE);
        String resourceName = (String) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_NAME);
        TungstenProperties resourceProperties = (TungstenProperties) monitorNotification
                .get(NotificationPropertyKey.KEY_RESOURCE_PROPERTIES);

//        if (resourceType == ResourceType.REPLICATOR)
//        {
//            return (ClusterResourceNotification) new ReplicatorNotification(
//                    clusterName, memberName, notificationSource, resourceName,
//                    resourceState, resourceProperties);
//        }
        if (resourceType == ResourceType.DATASOURCE)
        {
            
            return (ClusterResourceNotification) new DataSourceNotification(
                    clusterName, memberName, resourceName, resourceState,
                    notificationSource, resourceProperties);
        }
        else if (resourceType == ResourceType.DATASERVER)
        {
            return (ClusterResourceNotification) new DataServerNotification(
                    clusterName, memberName, resourceName, resourceState,
                    notificationSource, null);
        }
        else
        {
            return null;
        }
    }

}
