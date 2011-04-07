/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.cluster.resource.logical;

import java.util.Map;
import java.util.TreeMap;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.directory.DirectoryType;
import com.continuent.tungsten.commons.exception.ResourceException;

public class DataService extends Resource
{
    private static final long      serialVersionUID      = 1L;

    public static final String     DEFAULT_SHARD_NAME    = "all";
    public static final String     PROPS_SITENAME        = "siteName";
    public static final String     PROPS_CLUSTERNAME     = "clusterName";
    public static final String     PROPS_DATASERVICENAME = "dataServiceName";
    public static final String     PROPS_USER            = "user";
    public static final String     PROPS_PASSWORD        = "password";
    public static final String     PROPS_FQN             = "fqn";
    public static final String     PROPS_MASTER          = "master";
    public static final String     PROPS_SLAVE           = "slave";
    public static final String     PROPS_PORT            = "port";
    public static final String     PROPS_DETACHED        = "detached";
    public static final String     PROPS_CHANNELS        = "channels";
    public static final String     PROPS_AUTO_ONLINE     = "auto";

    protected String               siteName              = null;
    protected String               clusterName           = null;
    protected String               user                  = null;
    protected String               password              = null;
    protected String               fqn                   = null;
    protected int                  port                  = -1;
    protected boolean              detached              = false;
    protected int                  channels              = 1;
    protected boolean              auto                  = true;

    private Map<String, DataShard> shards                = new TreeMap<String, DataShard>();

    public DataService(String dataServiceName)
    {
        super(ResourceType.DATASERVICE, dataServiceName);
    }

    /**
     * Creates a new <code>DataService</code> object
     *
     * @param siteName
     * @param clusterName
     * @param dataServiceName
     * @param fqn
     * @param user
     * @param password
     * @param port TODO
     */
    public DataService(String siteName, String clusterName,
            String dataServiceName, String fqn, String user, String password,
            int port)
    {
        super(ResourceType.DATASERVICE, dataServiceName);
        this.siteName = siteName;
        this.clusterName = clusterName;
        this.fqn = fqn;
        this.user = user;
        this.password = password;
        this.port = port;
        isContainer = true;

    }

    /**
     * Creates a new <code>DataService</code> object
     *
     * @param serviceProperties
     * @param dataSourceMap
     * @throws ResourceException
     */
    public DataService(TungstenProperties serviceProperties,
            Map<String, TungstenProperties> dataSourceMap)
            throws ResourceException
    {
        this(serviceProperties.getString(DataService.PROPS_SITENAME),
                serviceProperties.getString(DataService.PROPS_CLUSTERNAME),
                serviceProperties.getString(DataService.PROPS_DATASERVICENAME),
                serviceProperties.getString(DataService.PROPS_FQN),
                serviceProperties.getString(DataService.PROPS_USER),
                serviceProperties.getString(DataService.PROPS_PASSWORD),
                serviceProperties.getInt(DataService.PROPS_PORT));

        this.setChannels(serviceProperties.getInt(DataService.PROPS_CHANNELS,
                "1", false));
        this.setAuto(serviceProperties.getBoolean(
                DataService.PROPS_AUTO_ONLINE, "true", true));

        this.setProperties(serviceProperties);

        DataShard defaultShard = new DataShard(this, DEFAULT_SHARD_NAME,
                dataSourceMap);

        addShard(defaultShard);

    }

    /**
     * Creates a new <code>DataService</code> object
     *
     * @param site
     * @param cluster
     * @param dataServiceName
     * @param dataSourceMap
     * @throws ResourceException
     */
    public DataService(String site, String cluster, String dataServiceName,
            Map<String, TungstenProperties> dataSourceMap)
            throws ResourceException
    {

        this(site, cluster, dataServiceName, null, null, null, -1);

        DataShard defaultShard = new DataShard(this, DEFAULT_SHARD_NAME,
                dataSourceMap);

        addShard(defaultShard);

    }

    private void addShard(DataShard shard)
    {
        shards.put(shard.getName(), shard);
    }

    /**
     * Returns the login value.
     *
     * @return Returns the login.
     */
    public String getUser()
    {
        return user;
    }

    /**
     * Sets the login value.
     *
     * @param login The login to set.
     */
    public void setUser(String login)
    {
        this.user = login;
    }

    /**
     * Returns the password value.
     *
     * @return Returns the password.
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password value.
     *
     * @param password The password to set.
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Returns the fqn value.
     *
     * @return Returns the fqn.
     */
    public String getFqn()
    {
        return fqn;
    }

    /**
     * Returns a representation of the fully qualified name that does not
     * contain slashes or colons but, instead, replaces those with a simple
     * underscore.
     */
    public String getCanonicalFQN()
    {
        return String.format("%s_%s_%s", getSiteName(), getClusterName(),
                getName());
    }

    /**
     * Sets the fqn value.
     *
     * @param fqn The fqn to set.
     */
    public void setFqn(String fqn)
    {
        this.fqn = fqn;
    }

    /**
     * Returns the defaultShard value.
     *
     * @return Returns the defaultShard.
     */
    public DataShard getDefaultShard()
    {
        return shards.get(DEFAULT_SHARD_NAME);
    }

    /**
     * Get all shards.
     */
    public Map<String, DataShard> getShards()
    {
        return shards;
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#getAllFacets()
     */
    public Map<String, DataShardFacet> getAllFacets()
    {
        return getDefaultShard().getAllFacets();
    }

    /**
     * @throws ResourceException
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#getCurrentMaster()
     */
    public DataShardFacet getMasterFacet() throws ResourceException
    {
        return getDefaultShard().getMasterFacet();
    }

    /**
     * @throws ResourceException
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#getCurrentMaster()
     */
    public DataSource getCurrentMaster() throws ResourceException
    {
        return getDefaultShard().getCurrentMaster();
    }

    /**
     * @param dsName
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#isActiveMaster(java.lang.String)
     */
    public boolean isActiveMaster(String dsName)
    {
        return getDefaultShard().isActiveMaster(dsName);
    }

    /**
     * @param dsName
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#isActiveSlave(java.lang.String)
     */
    public boolean isActiveSlave(String dsName)
    {
        return getDefaultShard().isActiveSlave(dsName);
    }

    /**
     * @param dsToRemove
     * @throws ResourceException
     * @see com.continuent.tungsten.commons.cluster.resource.logical.DataShard#removeDataSource(com.continuent.tungsten.commons.cluster.resource.physical.DataSource)
     */
    public void removeDataSource(DataSource dsToRemove)
            throws ResourceException
    {
        getDefaultShard().removeDataSource(dsToRemove);
    }

    public Map<String, DataSource> getAllDataSources()
    {
        Map<String, DataShardFacet> facets = getDefaultShard().getAllFacets();

        Map<String, DataSource> dataSources = new TreeMap<String, DataSource>();
        for (DataShardFacet facet : facets.values())
        {
            dataSources.put(facet.getName(), facet.getDataSource());
        }

        return dataSources;
    }

    /**
     * Returns the siteName value.
     *
     * @return Returns the siteName.
     */
    public String getSiteName()
    {
        return siteName;
    }

    /**
     * Sets the siteName value.
     *
     * @param siteName The siteName to set.
     */
    public void setSiteName(String siteName)
    {
        this.siteName = siteName;
    }

    /**
     * Returns the clusterName value.
     *
     * @return Returns the clusterName.
     */
    public String getClusterName()
    {
        return clusterName;
    }

    /**
     * Sets the clusterName value.
     *
     * @param clusterName The clusterName to set.
     */
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    /**
     * Returns the port value.
     *
     * @return Returns the port.
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Sets the port value.
     *
     * @param port The port to set.
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    public String toString()
    {
        if (directoryType == DirectoryType.LOGICAL)
            return getName() + "/";
        else
            return super.toString();
    }

    /**
     * Returns the detached value.
     *
     * @return Returns the detached.
     */
    public boolean isDetached()
    {
        return detached;
    }

    /**
     * Sets the detached value.
     *
     * @param detached The detached to set.
     */
    public void setDetached(boolean detached)
    {
        this.detached = detached;
    }

    /**
     * Returns the channels value.
     *
     * @return Returns the queues.
     */
    public int getChannels()
    {
        return channels;
    }

    /**
     * Sets the queues value.
     *
     * @param channels The channels to set.
     */
    public void setChannels(int channels)
    {
        this.channels = channels;
    }

    /**
     * Returns the auto-online value.
     *
     * @return Returns whether data service should auto-online.
     */
    public boolean isAuto()
    {
        return auto;
    }

    /**
     * Sets the auto-online value.
     *
     * @param auto The value to set.
     */
    public void setAuto(boolean auto)
    {
        this.auto = auto;
    }
}
