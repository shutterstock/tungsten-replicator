/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Contributor(s):
 */

package com.continuent.tungsten.commons.cluster.resource.logical;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.directory.DirectoryType;
import com.continuent.tungsten.commons.patterns.order.HighWaterResource;
import com.continuent.tungsten.commons.patterns.order.Sequence;

/**
 * Defines the fields used to represent status of a shard.
 */
public class DataShardFacet extends Resource
{
    /**
     *
     */
    private static final long  serialVersionUID = 1L;

    private DataShard          parent;
    private DataSource         dataSource;
    private DataShardFacetRole role;
    private DataShardFacetType facetType        = DataShardFacetType.local;

    public DataShardFacet()
    {
        super(ResourceType.DATASHARDFACET, "unknown");
    }

    public DataShardFacet(DataShard parent, DataSource dataSource)
    {
        super(ResourceType.DATASHARDFACET, dataSource.getName());
        this.parent = parent;
        this.dataSource = dataSource;
        this.setRole(dataSource.getRole());
    }

    /**
     * Returns the parent value.
     *
     * @return Returns the parent.
     */
    public DataShard getParent()
    {
        return parent;
    }

    /**
     * Sets the parent value.
     *
     * @param parent The parent to set.
     */
    public void setParent(DataShard parent)
    {
        this.parent = parent;
    }

    /**
     * Sets the role value.
     *
     * @param role The role to set.
     */
    public void setRole(DataShardFacetRole role)
    {
        this.role = role;
    }

    public String getFacetType()
    {
        return facetType.toString();
    }

    /**
     * Returns the dataSource value.
     *
     * @return Returns the dataSource.
     */
    public DataSource getDataSource()
    {
        return dataSource;
    }

    /**
     * Sets the dataSource value.
     *
     * @param dataSource The dataSource to set.
     */
    public void setDataSource(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    /**
     * @param destination
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#copyTo(com.continuent.tungsten.commons.cluster.resource.Resource)
     */
    public Resource copyTo(Resource destination)
    {
        return dataSource.copyTo(destination);
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#decrementActiveConnections()
     */
    public void decrementActiveConnections()
    {
        dataSource.decrementActiveConnections();
    }

    /**
     * @param detailed
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#describe(boolean)
     */
    public String describe(boolean detailed)
    {
        return dataSource.describe(detailed);
    }

    /**
     * @throws InterruptedException
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#disable()
     */
    public void disable() throws InterruptedException
    {
        dataSource.disable();
    }

    /**
     * @param obj
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj)
    {
        return dataSource.equals(obj);
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getActiveConnections()
     */
    public long getActiveConnections()
    {
        return dataSource.getActiveConnections();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getAppliedLatency()
     */
    public double getAppliedLatency()
    {
        return dataSource.getAppliedLatency();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getCallableStatementsCreated()
     */
    public long getCallableStatementsCreated()
    {
        return dataSource.getCallableStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getChildType()
     */
    public ResourceType getChildType()
    {
        return dataSource.getChildType();
    }

    /**
     * TODO: getClusterName definition.
     */
    public String getClusterName()
    {
        return parent.getParent().getClusterName();
    }

    /**
     * TODO: getSiteName definition.
     */
    public String getSiteName()
    {
        return parent.getParent().getSiteName();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getConnectionsCreated()
     */
    public long getConnectionsCreated()
    {
        return dataSource.getConnectionsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getDataSourceRole()
     */
    public DataShardFacetRole getDataSourceRole()
    {
        return dataSource.getDataSourceRole();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getDirectoryType()
     */
    public DirectoryType getDirectoryType()
    {
        return dataSource.getDirectoryType();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getDriver()
     */
    public String getDriver()
    {
        return dataSource.getDriver();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getEnabled()
     */
    public AtomicInteger getEnabled()
    {
        return dataSource.getEnabled();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getHighWater()
     */
    public HighWaterResource getHighWater()
    {
        return dataSource.getHighWater();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getHost()
     */
    public String getHost()
    {
        return dataSource.getHost();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getIsAvailable()
     */
    public boolean getIsAvailable()
    {
        return dataSource.getIsAvailable();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getKey()
     */
    public String getKey()
    {
        return dataSource.getKey();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getLastError()
     */
    public String getLastError()
    {
        return dataSource.getLastError();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getLastShunReason()
     */
    public String getLastShunReason()
    {
        return dataSource.getLastShunReason();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getLastUpdate()
     */
    public Date getLastUpdate()
    {
        return dataSource.getLastUpdate();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getPassword()
     */
    public String getPassword()
    {
        return dataSource.getPassword();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getPrecedence()
     */
    public int getPrecedence()
    {
        return dataSource.getPrecedence();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getPreparedStatementsCreated()
     */
    public long getPreparedStatementsCreated()
    {
        return dataSource.getPreparedStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getRole()
     */
    public String getRole()
    {
        return this.role.toString().toLowerCase();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getSequence()
     */
    public Sequence getSequence()
    {
        return dataSource.getSequence();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getState()
     */
    public ResourceState getState()
    {
        return dataSource.getState();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getStatementsCreated()
     */
    public long getStatementsCreated()
    {
        return dataSource.getStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getType()
     */
    public ResourceType getType()
    {
        return dataSource.getType();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getUrl()
     */
    public String getUrl()
    {
        return dataSource.getUrl();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getUser()
     */
    public String getUser()
    {
        return dataSource.getUser();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getVendor()
     */
    public String getVendor()
    {
        return dataSource.getVendor();
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode()
    {
        return dataSource.hashCode();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#incrementActiveConnections()
     */
    public void incrementActiveConnections()
    {
        dataSource.incrementActiveConnections();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#incrementCallableStatementsCreated()
     */
    public void incrementCallableStatementsCreated()
    {
        dataSource.incrementCallableStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#incrementConnectionsCreated()
     */
    public void incrementConnectionsCreated()
    {
        dataSource.incrementConnectionsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#incrementPreparedStatementsCreated()
     */
    public void incrementPreparedStatementsCreated()
    {
        dataSource.incrementPreparedStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#incrementStatementsCreated()
     */
    public void incrementStatementsCreated()
    {
        dataSource.incrementStatementsCreated();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#isAvailable()
     */
    public boolean isAvailable()
    {
        return dataSource.isAvailable();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#isContainer()
     */
    public boolean isContainer()
    {
        return dataSource.isContainer();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#isExecutable()
     */
    public boolean isExecutable()
    {
        return dataSource.isExecutable();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#isMaster()
     */
    public boolean isMaster()
    {
        return dataSource.isMaster();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#isSlave()
     */
    public boolean isSlave()
    {
        return dataSource.isSlave();
    }

    /**
     * @param dsProperties
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#set(java.util.Map)
     */
    public void set(Map<String, String> dsProperties)
    {
        dataSource.set(dsProperties);
    }

    /**
     * @param appliedLatency
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setAppliedLatency(double)
     */
    public void setAppliedLatency(double appliedLatency)
    {
        dataSource.setAppliedLatency(appliedLatency);
    }

    /**
     * @param childType
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setChildType(com.continuent.tungsten.commons.cluster.resource.ResourceType)
     */
    public void setChildType(ResourceType childType)
    {
        dataSource.setChildType(childType);
    }

    /**
     * @param cluster
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setCluster(java.lang.String)
     */
    public void setCluster(String cluster)
    {
        dataSource.setCluster(cluster);
    }

    /**
     * @param isContainer
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setContainer(boolean)
     */
    public void setContainer(boolean isContainer)
    {
        dataSource.setContainer(isContainer);
    }

    /**
     * @param role
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setDataSourceRole(com.continuent.tungsten.commons.cluster.resource.logical.DataShardFacetRole)
     */
    public void setDataSourceRole(DataShardFacetRole role)
    {
        dataSource.setDataSourceRole(role);
    }

    /**
     * @param directoryType
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setDirectoryType(com.continuent.tungsten.commons.directory.DirectoryType)
     */
    public void setDirectoryType(DirectoryType directoryType)
    {
        dataSource.setDirectoryType(directoryType);
    }

    /**
     * @param driver
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setDriver(java.lang.String)
     */
    public void setDriver(String driver)
    {
        dataSource.setDriver(driver);
    }

    /**
     * @param enabled
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setEnabled(java.util.concurrent.atomic.AtomicInteger)
     */
    public void setEnabled(AtomicInteger enabled)
    {
        dataSource.setEnabled(enabled);
    }

    /**
     * @param isExecutable
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setExecutable(boolean)
     */
    public void setExecutable(boolean isExecutable)
    {
        dataSource.setExecutable(isExecutable);
    }

    /**
     * @param error
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setFailed(java.lang.String)
     */
    public void setFailed(String error)
    {
        dataSource.setFailed(error);
    }

    /**
     * @param highWater
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setHighWater(com.continuent.tungsten.commons.patterns.order.HighWaterResource)
     */
    public void setHighWater(HighWaterResource highWater)
    {
        dataSource.setHighWater(highWater);
    }

    /**
     * @param epoch
     * @param eventId
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setHighWater(long,
     *      java.lang.String)
     */
    public void setHighWater(long epoch, String eventId)
    {
        dataSource.setHighWater(epoch, eventId);
    }

    /**
     * @param host
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setHost(java.lang.String)
     */
    public void setHost(String host)
    {
        dataSource.setHost(host);
    }

    /**
     * @param isAvailable
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setIsAvailable(boolean)
     */
    public void setIsAvailable(boolean isAvailable)
    {
        dataSource.setIsAvailable(isAvailable);
    }

    /**
     * @param lastError
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setLastError(java.lang.String)
     */
    public void setLastError(String lastError)
    {
        dataSource.setLastError(lastError);
    }

    /**
     * @param lastShunReason
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setLastShunReason(java.lang.String)
     */
    public void setLastShunReason(String lastShunReason)
    {
        dataSource.setLastShunReason(lastShunReason);
    }

    /**
     * @param name
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setName(java.lang.String)
     */
    public void setName(String name)
    {
        dataSource.setName(name);

    }

    public String getName()
    {
        return dataSource.getName();
    }

    /**
     * Returns the replicator host for this facet.
     */
    public String getReplicatorHost()
    {
        return dataSource.getReplicatorHost();
    }

    /**
     * Returns whether or not this facet has a remote replicator.
     */
    public boolean isRemoteReplicator()
    {
        return !getReplicatorHost().equals(getName());
    }

    /**
     * @param password
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setPassword(java.lang.String)
     */
    public void setPassword(String password)
    {
        dataSource.setPassword(password);
    }

    /**
     * @param precedence
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setPrecedence(int)
     */
    public void setPrecedence(int precedence)
    {
        dataSource.setPrecedence(precedence);
    }

    /**
     * @param role
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setRole(java.lang.String)
     */
    public void setRole(String role)
    {
        this.role = DataShardFacetRole.valueOf(role);
    }

    /**
     * @param sequence
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setSequence(com.continuent.tungsten.commons.patterns.order.Sequence)
     */
    public void setSequence(Sequence sequence)
    {
        dataSource.setSequence(sequence);
    }

    /**
     * @param reason
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setShunned(java.lang.String)
     */
    public void setShunned(String reason)
    {
        dataSource.setShunned(reason);
    }

    /**
     * @param state
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setState(com.continuent.tungsten.commons.cluster.resource.ResourceState)
     */
    public void setState(ResourceState state)
    {
        dataSource.setState(state);
    }

    /**
     * @param state
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setState(java.lang.String)
     */
    public void setState(String state)
    {
        dataSource.setState(state);
    }

    /**
     * @param type
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#setType(com.continuent.tungsten.commons.cluster.resource.ResourceType)
     */
    public void setType(ResourceType type)
    {
        dataSource.setType(type);
    }

    /**
     * @param url
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setUrl(java.lang.String)
     */
    public void setUrl(String url)
    {
        dataSource.setUrl(url);
    }

    /**
     * @param user
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setUser(java.lang.String)
     */
    public void setUser(String user)
    {
        dataSource.setUser(user);
    }

    /**
     * @param vendor
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setVendor(java.lang.String)
     */
    public void setVendor(String vendor)
    {
        dataSource.setVendor(vendor);
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#toMap()
     */
    public Map<String, String> toMap()
    {
        return dataSource.toMap();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#toProperties()
     */
    public TungstenProperties toProperties()
    {
        return dataSource.toProperties();
    }

    /**
     * @param ds
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#update(com.continuent.tungsten.commons.cluster.resource.physical.DataSource)
     */
    public void update(DataSource ds)
    {
        dataSource.update(ds);
    }

    public String getDiskLogDir()
    {
        return dataSource.getDiskLogDir();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getLogDir()
     */
    public String getLogDir()
    {
        return dataSource.getLogDir();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getLogPattern()
     */
    public String getLogPattern()
    {
        return dataSource.getLogPattern();
    }

    /**
     * @param logDir
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setLogDir(java.lang.String)
     */
    public void setLogDir(String logDir)
    {
        dataSource.setLogDir(logDir);
    }

    /**
     * @param logPattern
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setLogPattern(java.lang.String)
     */
    public void setLogPattern(String logPattern)
    {
        dataSource.setLogPattern(logPattern);
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#getPort()
     */
    public int getPort()
    {
        return dataSource.getPort();
    }

    /**
     * @param port
     * @see com.continuent.tungsten.commons.cluster.resource.physical.DataSource#setPort(int)
     */
    public void setPort(int port)
    {
        dataSource.setPort(port);
    }

    public String toString()
    {
        return String.format("%s(%s:%s)", getName(), getRole(), getState());
    }

    public DataShardFacet copy()
    {
        DataShardFacet copy = new DataShardFacet();

        copy.setParent(parent);
        copy.setDataSource(dataSource);
        copy.setName(getName());
        copy.setRole(getRole());
        return copy;

    }

    /**
     * Sets the facetType value.
     *
     * @param facetType The facetType to set.
     */
    public void setFacetType(DataShardFacetType facetType)
    {
        this.facetType = facetType;
    }

    public int getServicePort()
    {
        return parent.getParent().getPort();
    }

    public String getServiceName()
    {
        return parent.getParent().getName();
    }

    public String getCanonicalServiceFQN()
    {
        return parent.getParent().getCanonicalFQN();
    }

    public String getServiceUser()
    {
        return parent.getParent().getUser();
    }

    public String getServicePassword()
    {
        return parent.getParent().getPassword();
    }

    public boolean getDetached()
    {
        return parent.getParent().isDetached();
    }
}
