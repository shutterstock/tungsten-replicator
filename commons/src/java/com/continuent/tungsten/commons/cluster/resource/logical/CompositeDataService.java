
package com.continuent.tungsten.commons.cluster.resource.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exception.ResourceException;

public class CompositeDataService extends DataService
{
    private CompositeDataSource              master;
    private Map<String, CompositeDataSource> slaves           = new TreeMap<String, CompositeDataSource>();

    /**
     * 
     */
    private static final long                serialVersionUID = 1L;

    public CompositeDataService(String siteName, String clusterName,
            String dataServiceName, DataService master, List<DataService> slaveDsList)
    {
        super(dataServiceName);
        setFqn(dataServiceName);
        setSiteName(siteName);
        setClusterName(clusterName);
        setContainer(true);

        this.master = new CompositeDataSource(master.getFqn(), master,
                DataShardFacetRole.master);
        for (DataService slave : slaveDsList)
        {
            slaves.put(slave.getFqn(), new CompositeDataSource(slave.getFqn(),
                    slave, DataShardFacetRole.slave));
        }
    }

    public List<CompositeDataSource> getDataSourceList()
    {
        List<CompositeDataSource> list = new ArrayList<CompositeDataSource>();

        list.add(master);
        list.addAll(slaves.values());
        return list;
    }

    /**
     * Returns the master value.
     * 
     * @return Returns the master.
     */
    public CompositeDataSource getMaster()
    {
        return master;
    }

    /**
     * Sets the master value.
     * 
     * @param master The master to set.
     */
    public void setMaster(CompositeDataSource master)
    {
        this.master = master;
    }

    /**
     * Returns the slaves value.
     * 
     * @return Returns the slaves.
     */
    public List<CompositeDataSource> getSlaves()
    {
        return new ArrayList<CompositeDataSource>(slaves.values());
    }

    /**
     * Sets the slaves value.
     * 
     * @param slaves The slaves to set.
     */
    public void setSlaves(Map<String, CompositeDataSource> slaves)
    {
        this.slaves = slaves;
    }

    public Map<String, DataShardFacet> getAllSlaveFacets()
    {

        Map<String, DataShardFacet> facets = new HashMap<String, DataShardFacet>();

        for (CompositeDataSource slave : slaves.values())
        {
            for (DataShardFacet facet : slave.getAllFacets())
            {
                if (DataShardFacetRole.valueOf(facet.getRole()) == DataShardFacetRole.master)
                {
                    DataShardFacet slaveFacetCopy = facet.copy();
                    slaveFacetCopy.setRole(DataShardFacetRole.slave);
                    slaveFacetCopy.setFacetType(DataShardFacetType.remote);
                }
                facets.put(facet.getName(), facet);
            }
        }

        return facets;
    }

    public Map<String, DataShardFacet> getAllFacets()
    {
        Map<String, DataShardFacet> facets = new HashMap<String, DataShardFacet>();

        try
        {
            for (DataShardFacet facet : master.getAllFacets())
            {
                facets.put(facet.getName(), facet);
            }

            facets.putAll(getAllSlaveFacets());
        }
        catch (Exception e)
        {

        }

        return facets;

    }

    public DataShardFacet getMasterFacet() throws ResourceException
    {
        return getMaster().getDataService().getMasterFacet();
    }

    public TungstenProperties getProperties()
    {
        TungstenProperties props = new TungstenProperties();
        props.setString(DataService.NAME, getName());
        props.setString(DataService.TYPE, "composite");
        props.setString("master", getMaster().getName());
        String slave = "";
        for (CompositeDataSource slaveDs : slaves.values())
        {
            if (slave.length() > 0)
                slave += ",";

            slave += slaveDs.getDataService().getFqn();
        }
        props.setString("slave", slave);
        props.setInt(DataService.PROPS_PORT, getPort());
        return props;
    }
}
