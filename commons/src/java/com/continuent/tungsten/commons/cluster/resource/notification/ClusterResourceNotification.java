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

import java.io.Serializable;
import java.util.Date;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

public abstract class ClusterResourceNotification implements Serializable
{
    /**
     * 
     */
    private static final long      serialVersionUID    = 1L;
    protected ResourceType         resourceType        = ResourceType.UNDEFINED;
    protected NotificationStreamID streamID            = NotificationStreamID.ANY;
    protected long                 timeSent            = 0L;
    protected long                 timeReceived        = 0L;
    protected String               clusterName         = null;
    protected String               memberName          = null;
    protected String               resourceName        = null;
    protected ResourceState        resourceState       = null;
    protected String               notificationSource  = null;
    protected TungstenProperties   resourceProps       = null;
    protected Resource             resource;

    public static final String     RESOURCE_TYPE       = "resourceType";
    public static final String     CLUSTER_NAME        = "clusterName";
    public static final String     MEMBER_NAME         = "memberName";
    public static final String     RESOURCE_NAME       = "resourceName";
    public static final String     RESOURCE_STATE      = "resourceState";
    public static final String     NOTIFICATION_SOURCE = "notificationSource";
    public static final String     RESOURCE_PROPS      = "resourceProps";

    public ClusterResourceNotification(String clusterName, String memberName,
            String notificationSource, ResourceType resourceType,
            String resourceName, ResourceState resourceState,
            TungstenProperties resourceProps)
    {
        this(NotificationStreamID.MONITORING, clusterName, memberName,
                notificationSource, resourceType, resourceName, resourceState,
                resourceProps);
    }

    public ClusterResourceNotification(NotificationStreamID streamID,
            String clusterName, String memberName, String notificationSource,
            ResourceType resourceType, String resourceName,
            ResourceState resourceState, TungstenProperties resourceProps)
    {
        this.resourceType = resourceType;
        this.clusterName = clusterName;
        this.memberName = memberName;
        this.resourceName = resourceName;
        this.resourceState = resourceState;
        this.notificationSource = notificationSource;
        this.resourceProps = resourceProps;
    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString(RESOURCE_TYPE, resourceType.toString());
        props.setString(CLUSTER_NAME, clusterName);
        props.setString(MEMBER_NAME, memberName);
        props.setString(RESOURCE_NAME, resourceName);
        props.setString(RESOURCE_STATE, resourceState.toString());
        props.setString(NOTIFICATION_SOURCE, notificationSource);
        props.setObject(RESOURCE_PROPS, resourceProps);
        return props;
    }

    public ResourceType getType()
    {
        return resourceType;
    }

    public Resource getResource()
    {
        return resource;
    }

    public String getSource()
    {
        return notificationSource;
    }

    public ResourceState getState()
    {
        return resourceState;
    }

    public String getResourceName()
    {
        return resourceName;
    }

    public TungstenProperties getResourceProps()
    {
        return resourceProps;
    }

    public void setResource(Resource resource)
    {
        this.resource = resource;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public ResourceType getResourceType()
    {
        return resourceType;
    }

    public void setResourceType(ResourceType resourceType)
    {
        this.resourceType = resourceType;
    }

    public ResourceState getResourceState()
    {
        return resourceState;
    }

    public void setResourceState(ResourceState resourceState)
    {
        this.resourceState = resourceState;
    }

    public String getNotificationSource()
    {
        return notificationSource;
    }

    public void setNotificationSource(String notificationSource)
    {
        this.notificationSource = notificationSource;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    public void setResourceName(String resourceName)
    {
        this.resourceName = resourceName;
    }

    public void setResourceProps(TungstenProperties resourceProps)
    {
        this.resourceProps = resourceProps;
    }

    public String getMemberName()
    {
        return memberName;
    }

    public void setMemberName(String memberName)
    {
        this.memberName = memberName;
    }

    public long getTimeSent()
    {
        return timeSent;
    }

    public void setTimeSent(long timeSent)
    {
        this.timeSent = timeSent;
    }

    public long getTimeReceived()
    {
        return timeReceived;
    }

    public void setTimeReceived(long timeReceived)
    {
        this.timeReceived = timeReceived;
    }

    public String toString()
    {
        // type cluster/name(state
        return String.format("[%s] %s %s/%s(state=%s)", new Date(timeReceived)
                .toString(), resourceType, clusterName, resourceName,
                resourceState);
    }
}
