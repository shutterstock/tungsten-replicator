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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exception.ResourceException;
import com.continuent.tungsten.commons.utils.ResultFormatter;

/**
 * Defines the fields used to represent status of a shard.
 */
public class DataShard extends Resource
{
    /**
     * 
     */
    private static final long                      serialVersionUID = 1L;

    private DataService                            parent;

    private Map<String, DataShardFacet>            resources        = new TreeMap<String, DataShardFacet>();
    private Map<String, ArrayList<DataShardFacet>> resourcesByRole  = new HashMap<String, ArrayList<DataShardFacet>>();

    public DataShard(DataService parent, String shardName,
            Map<String, TungstenProperties> dataSourceMap)
            throws ResourceException
    {
        super(ResourceType.DATASHARD, shardName);
        isContainer = true;
        this.parent = parent;

        TungstenProperties masterProps = dataSourceMap.get(parent
                .getProperties().getString(DataService.PROPS_MASTER));

        masterProps.setString(DataSource.ROLE, DataShardFacetRole.master
                .toString());

        DataShardFacet newFacet = new DataShardFacet(this, new DataSource(
                masterProps));

        resources.put(newFacet.getName(), newFacet);
        addByRole(newFacet);

        String slaves = parent.getProperties().getString(
                DataService.PROPS_SLAVE);

        if (slaves != null)
        {
            for (String slaveName : slaves.split(","))
            {
                TungstenProperties slaveProps = dataSourceMap.get(slaveName
                        .trim());

                slaveProps.setString(DataSource.ROLE, DataShardFacetRole.slave
                        .toString());

                newFacet = new DataShardFacet(this, new DataSource(slaveProps));

                resources.put(newFacet.getName(), newFacet);
                addByRole(newFacet);
            }
        }
    }

    public boolean isActiveMaster(String dsName)
    {
        DataSource currentMaster = null;

        try
        {
            currentMaster = getCurrentMaster();
            if (currentMaster.getName().equals(dsName))
                return true;
        }
        catch (ResourceException ignored)
        {

        }

        return false;
    }

    public boolean isActiveSlave(String dsName)
    {
        DataShardFacet foundDs = null;

        try
        {
            foundDs = getFacet(dsName);

            if (foundDs.getRole().equals(DataShardFacetRole.slave.toString())
                    && !(foundDs.getState() == ResourceState.FAILED || foundDs
                            .getState() == ResourceState.SHUNNED))
                return true;

        }
        catch (ResourceException ignored)
        {

        }

        return false;
    }

    public DataShardFacet getMasterFacet() throws ResourceException
    {
        ArrayList<DataShardFacet> dsByRole = getRoleArray(DataShardFacetRole.master
                .toString());
        DataShardFacet foundDs = null;

        for (DataShardFacet ds : dsByRole)
        {
            if (ds.getRole().equals(DataShardFacetRole.master.toString()))
            {
                foundDs = ds;
            }
        }

        if (foundDs == null)
        {
            throw new ResourceException("No master is currently available");
        }

        return foundDs;
    }

    public DataSource getCurrentMaster() throws ResourceException
    {
        DataShardFacet facet = getMasterFacet();

        if (facet != null)
            return facet.getDataSource();

        return null;
    }

    public void removeDataSource(DataSource dsToRemove)
            throws ResourceException
    {

        DataShardFacet foundDs = resources.get(dsToRemove.getName());

        if (foundDs == null)
        {
            throw new ResourceException(String.format(
                    "Did not find a datasource named '%s' in service '%s'",
                    dsToRemove.getName(), getName()));
        }

        resources.remove(dsToRemove.getName());
        removeByRole(dsToRemove);

    }

    public DataShardFacet getFacet(String dsName) throws ResourceException
    {
        DataShardFacet ds = null;

        synchronized (resources)
        {
            ds = resources.get(dsName);
        }

        if (ds == null)
        {
            throw new ResourceException(String.format(
                    "Facet %s not found for service %s", dsName, getName()));
        }

        return ds;
    }

    public Map<String, DataShardFacet> getAllFacets()
    {
        synchronized (this)
        {
            return resources;
        }

    }

    private void addByRole(DataShardFacet ds) throws ResourceException
    {
        validate(ds);
        synchronized (this)
        {
            ArrayList<DataShardFacet> dsByRole = getRoleArray(ds.getRole());
            dsByRole.add(ds);
        }
    }

    private void removeByRole(DataSource ds)
    {
        ArrayList<DataShardFacet> dsByRole = getRoleArray(ds.getRole());

        int objIndex = -1;

        if ((objIndex = dsByRole.indexOf(ds)) >= 0)
        {
            dsByRole.remove(objIndex);
        }

    }

    private ArrayList<DataShardFacet> getRoleArray(String role)
    {

        ArrayList<DataShardFacet> dsByRole = resourcesByRole.get(role);

        if (dsByRole == null)
        {
            dsByRole = new ArrayList<DataShardFacet>();
            resourcesByRole.put(role, dsByRole);
        }

        return dsByRole;
    }

    private void validate(DataShardFacet facet) throws ResourceException
    {
        if ((facet.getName() == null || facet.getName().length() == 0)
                || (facet.getDriver() == null || facet.getDriver().length() == 0)
                || (facet.getUrl() == null || facet.getUrl().length() == 0)
                || (facet.getRole().equals(DataShardFacetRole.undefined
                        .toString())))
        {
            throw new ResourceException(String.format(
                    "Malformed datasource encountered: %s", facet.toString()));
        }
    }

    /**
     * Returns the parent value.
     * 
     * @return Returns the parent.
     */
    public DataService getParent()
    {
        return parent;
    }

    /**
     * Sets the parent value.
     * 
     * @param parent The parent to set.
     */
    public void setParent(DataService parent)
    {
        this.parent = parent;
    }

    public String describe(boolean detailed)
    {
        StringBuilder builder = new StringBuilder();
        String NEWLINE = "\n";
        String dataShardHeader = String.format("SHARD: %s", getName());
        builder.append(
                ResultFormatter.makeSeparator(ResultFormatter.DEFAULT_WIDTH, 1,
                        true)).append(NEWLINE);
        builder.append(ResultFormatter.makeRow((new String[]{dataShardHeader}),
                ResultFormatter.DEFAULT_WIDTH, 0, true, true));

        builder.append(
                ResultFormatter.makeSeparator(ResultFormatter.DEFAULT_WIDTH, 1,
                        true)).append(NEWLINE);

        int facetCount = 0;

        for (DataShardFacet facet : getAllFacets().values())
        {
            builder.append(ResultFormatter
                    .makeRow((new String[]{facet.toString()}),
                            ResultFormatter.DEFAULT_WIDTH, 0, true, true));
            facetCount++;
        }

        if (facetCount > 0)
            builder.append(
                    ResultFormatter.makeSeparator(
                            ResultFormatter.DEFAULT_WIDTH, 1, true)).append(
                    NEWLINE);

        return builder.toString();
    }

    /**
     * Shard name, which is a unique name within the service to which the shard
     * belongs.
     */

    public String toString()
    {
        return getName() + "/";
    }
}
