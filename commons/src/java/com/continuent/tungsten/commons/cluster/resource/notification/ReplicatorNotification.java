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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.Replicator;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class ReplicatorNotification extends ClusterResourceNotification
{

    /**
     * Used to determine if a given de-serialized object is compatible with this
     * class version.<br>
     * This value must be changed if and only if the new version of this class
     * is not compatible with old versions. See <a
     * href=http://java.sun.com/products/jdk/1.1/docs/guide
     * /serialization/spec/version.doc.html/> for a list of compatible changes.
     */
    private static final long               serialVersionUID    = -2097111546144612171L;

    private Map<String, TungstenProperties> replicationServices = null;

    /**
     * @param clusterName
     * @param memberName TODO
     * @param notificationSource
     * @param resourceName
     * @param resourceState
     * @param replicationServices
     */
    public ReplicatorNotification(String clusterName, String memberName,
            String notificationSource, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps,
            Map<String, TungstenProperties> replicationServices)
    {
        super(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, ResourceType.REPLICATION_SERVICE,
                resourceName, resourceState, resourceProps);

        this.replicationServices = replicationServices;
    }

    /**
     * Returns the replicationServices value.
     * 
     * @return Returns the replicationServices.
     */
    public Map<String, TungstenProperties> getReplicationServices()
    {
        return replicationServices;
    }

    /**
     * Sets the replicationServices value.
     * 
     * @param replicationServices The replicationServices to set.
     */
    public void setReplicationServices(
            Map<String, TungstenProperties> replicationServices)
    {
        this.replicationServices = replicationServices;
    }

    public String getDataServerHost()
    {
        return getResourceProps().getString(Replicator.DATASERVER_HOST);
    }

    public Collection<ReplicationServiceNotification> getServiceNotifications()
    {
        Collection<ReplicationServiceNotification> notifications = new ArrayList<ReplicationServiceNotification>();

        String source = getClass().getSimpleName() + ":connected";

        for (TungstenProperties props : getReplicationServices().values())
        {
            props.setString("notificationSource", getClass().getSimpleName()
                    + ":connected");
            ReplicationServiceNotification notification = new ReplicationServiceNotification(
                    props.getString(Replicator.CLUSTERNAME), props
                            .getString(Replicator.HOST), source, props
                            .getString(Replicator.SERVICE_NAME),
                    ReplicationServiceNotification
                            .replicatorStateToResourceState(props
                                    .getString(Replicator.STATE)), props);

            notifications.add(notification);
        }

        return notifications;
    }

    public String toString()
    {
        // type cluster/name(state
        return String.format("[%s] %s %s/%s(state=%s, dataServerHost=%s)", new Date(timeReceived)
                .toString(), resourceType, clusterName, resourceName,
                resourceState, getDataServerHost());

    }

}
