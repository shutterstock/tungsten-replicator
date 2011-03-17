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
 * Contributor(s): Robert Hodges.
 */

package com.continuent.tungsten.commons.cluster.resource.notification;

import java.util.Map;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Lists all states in which a resource can be found. This class instances are
 * created by the checker class and then sent to the notifier for processing.<br>
 * This class was based on tungsten sql router
 * com.continuent.tungsten.router.manager.RouterState
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class ResourceStatus
{
    /**
     * Cluster that this notification is from
     */
    private String             clusterName;

    /**
     * Cluster member that this notification is from
     */
    private String             clusterMemberName;
    /**
     * Type of RouterResource
     * 
     * @see Types
     */
    private String             type;
    /**
     * Possibly unique identifier for this resource
     */
    private String             name;
    /**
     * Last known state
     * 
     * @see States
     */
    private String             state;
    /**
     * RouterResource-specific properties
     */
    private TungstenProperties properties;

    /**
     * Creates a new <code>ResourceStatus</code> object, giving all fields
     * 
     * @param type Type of RouterResource
     * @param name Possibly unique identifier for this resource
     * @param state Last known state
     * @param properties RouterResource-specific properties
     */
    public ResourceStatus(String type, String name, String state,
            TungstenProperties properties)
    {
        this.type = type;
        this.name = name;
        this.state = state;
        this.properties = properties;
    }

    /**
     * Inner class enumerating the different possible RouterResource Types<br>
     * Types are defined as strings so that they can be sent with native java
     * functions by the notifier
     * 
     * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
     * @version 1.0
     */
    public static final class Types
    {
        public static final String DATASOURCE = "DATASOURCE";
        public static final String REPLICATOR = "REPLICATOR";
        public static final String DATASERVER = "DATASERVER";
        public static final String HOST       = "HOST";
        public static final String UNKNOWN    = "UNKNOWN";
    }

    /**
     * Inner class enumerating the different possible RouterResource States<br>
     * States are defined as strings so that they can be sent with native java
     * functions by the notifier
     * 
     * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
     * @version 1.0
     */
    public static final class States
    {
        public static final String ONLINE     = "ONLINE";
        public static final String OFFLINE    = "OFFLINE";
        public static final String NOTRUNNING = "STOPPED";
        public static final String UNKNOWN    = "UNKNOWN";
    }

    /**
     * Retrieves the name of the resource
     * 
     * @return the resource name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Retrieves the type of the resource
     * 
     * @return the resource type, on of {@link Types}
     */
    public String getType()
    {
        return type;
    }

    /**
     * Retrieves the state of the resource
     * 
     * @return the resource state, one of {@link States}
     */
    public String getState()
    {
        return state;
    }

    /**
     * Adds the given resource-specific properties to the current properties,
     * possibly replacing current values
     * 
     * @param additionalProps new properties to be added to the current ones
     */
    protected void setProperties(Map<String, String> additionalProps)
    {
        if (additionalProps != null)
        {
            if (properties == null)
                properties = new TungstenProperties();

            for (Object key : additionalProps.keySet())
            {
                Object value = additionalProps.get(key);
                if (value != null)
                    properties.put(key, value);
                else
                    System.out.println(String.format(
                            "Skipping: Null value for key %s found.", key));
            }
        }
    }

    /**
     * Retrieves the resource-specific properties
     * 
     * @return the resource properties containing resource specific information
     */
    public TungstenProperties getProperties()
    {
        return properties;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return "RouterResource name=" + getName() + " type=" + getType()
                + " state=" + getState() + " properties=" + getProperties();
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    public String getClusterMemberName()
    {
        return clusterMemberName;
    }

    public void setClusterMemberName(String clusterMemberName)
    {
        this.clusterMemberName = clusterMemberName;
    }
}
