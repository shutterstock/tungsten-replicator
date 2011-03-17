/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s):  Joe Daly
 * Contributor(s):
 */

package com.continuent.tungsten.commons.cluster.resource;

import java.io.Serializable;

import org.continuent.hedera.common.Member;

import com.continuent.tungsten.commons.patterns.order.Sequence;

/**
 * A wrapper to uniquely identify a member. A unique UUID identifier is created
 * to make comparisions easy, rather then having to check member name, host name
 * and group name.
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */
public class ClusterManagerID implements Serializable, Comparable<Object>
{
    static final long          serialVersionUID            = 1L;

    private Member             member;

    private String             siteName                    = null;
    private String             clusterName;
    private String             memberName;

    int                        weight;

    private String             uniqueIdentifier;

    // dummy id used for group communication messages
    public static final String GROUP_COMMUNICATION_MESSAGE = "GROUP_COMMUNICATION_MESSAGE";

    /**
     * Someones identity on the groupCommunicaton channel. View changes come
     * accross with this identity from the underlying system, existing members
     * can update the membership list based on this identity
     */
    String                     groupCommunicationIdentity;

    /**
     * A flag indicating our identity
     */
    private boolean            isMe;

    private boolean            isClusterMember             = false;

    private boolean            online                      = false;

    private Sequence           viewID                      = new Sequence(
                                                                   Long.MAX_VALUE);

    // the ip address of this member, this could be extracted from
    // the group comm identity but that could be in different formats
    private String             ipAddress;

    private int                port;

    /**
     * Creates a new ClusterManagerID
     * 
     * @param siteName TODO
     * @param memberName the user assigned name
     * @param weight a server weight, this is used to determine which member is
     *            the representitive
     * @param groupCommunicationIdentity the identity of this server in the
     *            underlying group communication system
     * @param port the port number used to connect to the manager
     * @param clusterName the group name
     */
    public ClusterManagerID(Member member, String siteName, String clusterName,
            String memberName, int weight, String groupCommunicationIdentity,
            int port)
    {
        this.member = member;
        this.siteName = siteName;
        this.clusterName = clusterName;
        this.memberName = memberName;
        this.weight = weight;

        this.uniqueIdentifier = memberName; // groupCommunicationIdentity;
        this.groupCommunicationIdentity = groupCommunicationIdentity;
        this.isMe = false;

        this.ipAddress = member.getAddress().toString();

        this.port = port;

    }

    /**
     * Get the group name for this server.
     * 
     * @return the group name
     */
    public String getClusterName()
    {
        if (clusterName == null)
            return "UNDEFINED";

        return this.clusterName;
    }

    /**
     * The current uuid
     * 
     * @return the current uuid
     */
    public String getUUID()
    {
        return this.uniqueIdentifier;
    }

    /**
     * Get the member name for this server.
     * 
     * @return the member name
     */
    public String getMemberName()
    {
        if (memberName == null)
            return "UNDEFINED";

        return this.memberName;
    }

    /**
     * Get the weight of this server.
     * 
     * @return the member weight
     */
    public int getWeight()
    {
        return this.weight;
    }

    /**
     * If this ClusterManagerID represents the local Service ResourceManager
     * this should return true
     * 
     * @return true if this ClusterManagerID belongs to the local Service
     *         ResourceManager
     */
    public boolean getIsMe()
    {
        return this.isMe;
    }

    /**
     * The underlying group communication identity
     * 
     * @return an indentity as the underlying group communcation knows us as
     */
    public String getGroupCommunicationIdentity()
    {
        return this.groupCommunicationIdentity;
    }

    public String getIpAddress()
    {
        return this.ipAddress;
    }

    public int getFileServerPort()
    {
        return this.port;
    }

    // Setters

    /**
     * Set whether this member is me.
     * 
     * @param isMe should be set to true if this server is the local server
     */
    public void setIsMe(boolean isMe)
    {
        this.isMe = isMe;
    }

    /**
     * Sets the group communication identity. This needs to be reset upon
     * leaving rejoining the group.
     * 
     * @param groupCommunicationIdentity sets the identity used by the
     *            underlying group communication
     */
    public void setGroupCommunicationIdentity(String groupCommunicationIdentity)
    {
        this.groupCommunicationIdentity = groupCommunicationIdentity;
    }

    // Utils

    public boolean equals(Object obj)
    {
        return compareTo(obj) == 0;
    }

    public int compareTo(Object obj)
    {
        ClusterManagerID id = (ClusterManagerID) obj;
        return this.uniqueIdentifier.compareTo(id.uniqueIdentifier);
    }

    public int hashCode()
    {
        return this.uniqueIdentifier.hashCode();
    }

    public String toString()
    {
        return String.format("%s.%s%s", clusterName, memberName,
                groupCommunicationIdentity);

    }

    public Member getMember()
    {
        return member;
    }

    public boolean isClusterMember()
    {
        return isClusterMember;
    }

    public void setClusterMember(boolean isClusterMember)
    {
        this.isClusterMember = isClusterMember;
    }

    public boolean isOnline()
    {
        return online;
    }

    public void setOnline(boolean online)
    {
        this.online = online;
    }

    public String getSiteName()
    {
        return siteName;
    }

    public void setSiteName(String siteName)
    {
        this.siteName = siteName;
    }

    /**
     * @return the viewID
     */
    public Sequence getViewID()
    {
        return viewID;
    }

    /**
     * @param viewID the viewID to set
     */
    public void setViewID(Sequence viewID)
    {
        this.viewID = viewID;
    }

    /**
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port)
    {
        this.port = port;
    }
}
