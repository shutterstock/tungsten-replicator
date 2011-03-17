
package com.continuent.tungsten.commons.directory;

import java.util.Vector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ClusterManagerID;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.logical.CompositeDataService;
import com.continuent.tungsten.commons.cluster.resource.logical.CompositeDataSource;
import com.continuent.tungsten.commons.cluster.resource.logical.DataService;
import com.continuent.tungsten.commons.cluster.resource.logical.DataShard;
import com.continuent.tungsten.commons.cluster.resource.logical.DataShardFacet;
import com.continuent.tungsten.commons.cluster.resource.logical.LogicalResourceFactory;
import com.continuent.tungsten.commons.exception.DirectoryNotFoundException;
import com.continuent.tungsten.commons.exception.ResourceException;

public class ClusterLogicalDirectory extends ClusterGenericDirectory
{
    /**
     * 
     */
    private static final long              serialVersionUID  = 1L;

    private static Logger                  logger            = Logger
                                                                     .getLogger(ClusterLogicalDirectory.class);

    private static final String            FOLDER_NAME_SHARD = "shards";
    private static final String            FOLDER_NAME_GLOBAL = "global";
  

    private ResourceNode                   globalFolder      = null;

    private static ClusterLogicalDirectory _instance         = null;

    private ClusterLogicalDirectory(DirectorySessionManager sessionManager,
            ClusterManagerID managerID) throws ResourceException,
            DirectoryNotFoundException
    {
        super(managerID, new LogicalResourceFactory(), DirectoryType.LOGICAL,
                sessionManager);

        globalFolder = getResourceFactory().addInstance(ResourceType.FOLDER,
                FOLDER_NAME_GLOBAL, getRootNode());
    }

    public static ClusterLogicalDirectory getInstance(
            ClusterManagerID managerID, DirectorySessionManager sessionManager)
            throws ResourceException, DirectoryNotFoundException
    {
        if (_instance == null)
        {
            _instance = new ClusterLogicalDirectory(sessionManager, managerID);
        }

        return _instance;
    }

    public void removeDataSource(String sessionID, String clusterName,
            String dataServiceName, String dataSourceName) throws Exception
    {
        ResourceNode dataSourceNode = getDataSource(sessionID, clusterName,
                dataServiceName, dataSourceName);

        if (dataSourceNode == null)
            return;

        dataSourceNode.unlink();

        dataSourceNode.getParent().removeChild(dataSourceNode.getKey());
    }

    public ResourceNode getDataSource(String sessionID, String clusterName,
            String dataServiceName, String dataSourceName) throws Exception
    {
        ResourceNode dataServiceNode = getDataService(sessionID, clusterName,
                dataServiceName);
        if (dataServiceNode == null)
            return null;

        ResourceNode dataSourcesNode = dataServiceNode.getChildren().get(
                "datasource");

        return dataSourcesNode.getChildren().get(dataSourceName);
    }

    public ResourceNode getCompositeDataService(String sessionID,
            String dataServiceName) throws Exception
    {
        return getGlobalFolder().getChildren().get(dataServiceName);
    }

    public ResourceNode getDataService(String sessionID, String clusterName,
            String dataServiceName) throws Exception
    {
        ResourceNode clusterNode = getClusterNode(sessionID, siteName,
                clusterName);

        return clusterNode.getChildren().get(dataServiceName);
    }

    public void removeDataService(String sessionID, String clusterName,
            String dataServiceName) throws Exception
    {
        ResourceNode dataService = getDataService(sessionID, clusterName,
                dataServiceName);

        if (dataService == null)
            return;

        dataService.getParent().removeChild(dataService.getKey());
    }

    public void removeCompositeDataService(String sessionID, String dataServiceName)
            throws Exception
    {
        ResourceNode dataService = getCompositeDataService(sessionID,
                dataServiceName);

        if (dataService == null)
            return;

        dataService.getParent().removeChild(dataService.getKey());
    }

    public void addDataService(String sessionID, DataService dataService)
            throws Exception
    {
        ResourceNode clusterNode = getClusterNode(sessionID, siteName,
                clusterName);
        ResourceNode dataServiceNode = clusterNode.addChild(dataService);
        populateDataServiceNode(dataServiceNode, dataService);

    }

    private void populateDataServiceNode(ResourceNode dataServiceNode,
            DataService dataService) throws Exception
    {
        ResourceNode shardFolder = getResourceFactory().addInstance(
                ResourceType.FOLDER, "shard", dataServiceNode);

        ResourceNode dataSourceFolder = getResourceFactory().addInstance(
                ResourceType.FOLDER, "datasource", dataServiceNode);

        Vector<ResourceNode> shardNodes = new Vector<ResourceNode>();

        for (DataShard shard : dataService.getShards().values())
        {
            ResourceNode shardNode = shardFolder.addChild(shard);
            shardNodes.add(shardNode);
        }

        for (DataShardFacet facet : dataService.getAllFacets().values())
        {
            ResourceNode facetNode = dataSourceFolder.addChild(facet);
            for (ResourceNode shardNode : shardNodes)
            {
                shardNode.link(facetNode);
            }
        }

    }

    public void addCompositeDataService(String sessionID,
            CompositeDataService dataService) throws Exception
    {
        ResourceNode dataServiceNode = getGlobalFolder().addChild(dataService);

        dataServiceNode.addChild(dataService.getMaster());

        for (CompositeDataSource slaveDs : dataService.getSlaves())
        {
            dataServiceNode.addChild(slaveDs);
        }
    }

    /**
     * Returns the globalFolder value.
     * 
     * @return Returns the globalFolder.
     */
    public ResourceNode getGlobalFolder()
    {
        return globalFolder;
    }

    /**
     * Sets the globalFolder value.
     * 
     * @param globalFolder The globalFolder to set.
     */
    public void setGlobalFolder(ResourceNode globalFolder)
    {
        this.globalFolder = globalFolder;
    }

}
