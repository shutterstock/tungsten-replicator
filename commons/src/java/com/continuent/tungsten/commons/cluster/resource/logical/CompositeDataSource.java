
package com.continuent.tungsten.commons.cluster.resource.logical;

import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;

public class CompositeDataSource extends Resource
{
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;

    private DataShardFacetRole role;
    private DataService        dataService;

    public CompositeDataSource(String name)
    {
        super(ResourceType.COMPOSITE_DATASOURCE, name);
    }

    public CompositeDataSource(String name, DataService dataService,
            DataShardFacetRole role)
    {
        this(name);
        setDataService(dataService);
        setRole(role);
    }

    /**
     * Returns the role value.
     * 
     * @return Returns the role.
     */
    public DataShardFacetRole getRole()
    {
        return role;
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

    /**
     * Returns the dataService value.
     * 
     * @return Returns the dataService.
     */
    public DataService getDataService()
    {
        return dataService;
    }

    /**
     * Sets the dataService value.
     * 
     * @param dataService The dataService to set.
     */
    public void setDataService(DataService dataService)
    {
        this.dataService = dataService;
    }

    public List<DataShardFacet> getAllFacets()
    {
        List<DataShardFacet> facets = new ArrayList<DataShardFacet>();
        facets.addAll(dataService.getAllFacets().values());
        return facets;

    }

    public String toString()
    {
        try
        {
            DataSource masterDs = dataService.getCurrentMaster();
            if (masterDs != null)
            {
                return String.format("%s:%s@%s", role.toString(), dataService
                        .getFqn(), masterDs.toString());
            }
        }
        catch (Exception e)
        {

        }

        return String.format("%s:%s@%s", role.toString(), dataService.getFqn(),
                "UNAVAILABLE");

    }
}
