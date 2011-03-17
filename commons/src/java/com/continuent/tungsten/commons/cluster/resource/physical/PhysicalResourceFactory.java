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

package com.continuent.tungsten.commons.cluster.resource.physical;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.shared.Cluster;
import com.continuent.tungsten.commons.cluster.resource.shared.Queue;
import com.continuent.tungsten.commons.cluster.resource.shared.Site;
import com.continuent.tungsten.commons.directory.ClusterGenericDirectory;
import com.continuent.tungsten.commons.directory.Directory;
import com.continuent.tungsten.commons.directory.DirectoryResourceFactory;
import com.continuent.tungsten.commons.directory.DirectoryType;
import com.continuent.tungsten.commons.directory.ResourceNode;
import com.continuent.tungsten.commons.exception.ResourceException;
import com.continuent.tungsten.commons.utils.ReflectUtils;

public class PhysicalResourceFactory extends DirectoryResourceFactory
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param type
     * @param key
     * @param parent
     * @return the node created/added
     * @throws ResourceException
     */
    public ResourceNode addInstance(ResourceType type, String key,
            ResourceNode parent) throws ResourceException
    {

        Resource newInstance = createInstance(type, key, parent);

        return parent.addChild(newInstance);
    }

    /**
     * Makes a copy of the source RouterResource and returns it with the new key
     * set.
     * 
     * @param source
     * @param destinationKey
     * @param destination
     * @return a copy of the resource
     * @throws ResourceException
     */
    public Resource copyInstance(ResourceNode source, String destinationKey,
            ResourceNode destination) throws ResourceException
    {

        Resource copy = (Resource) ReflectUtils.clone(source.getResource());

        if (copy.getType() != destination.getResource().getChildType())
        {
            throw new ResourceException(
                    String
                            .format(
                                    "cannot create a copy of type '%s' in destination for type '%s'",
                                    copy.getType(), destination.getResource()
                                            .getChildType()));
        }

        copy.setName(destinationKey);
        return copy;
    }

    /**
     * Creates a new resource instance of the specified type
     * 
     * @param type
     * @param key
     * @param parent
     * @return the instance created
     * @throws ResourceException
     */
    public Resource createInstance(ResourceType type, String key,
            ResourceNode parent) throws ResourceException
    {
        Resource newInstance = null;

        if (type == ResourceType.SITE)
        {
            newInstance = createSite(key, parent);
        }
        else if (type == ResourceType.CLUSTER)
        {
            newInstance = createCluster(key, parent);
        }
        else if (type == ResourceType.MEMBER)
        {
            newInstance = createClusterMember(key, parent);
        }
        else if (type == ResourceType.PROCESS)
        {
            newInstance = createProcess(key, parent);
        }
        else if (type == ResourceType.RESOURCE_MANAGER)
        {
            newInstance = createResourceManager(key, parent);
        }
        else if (type == ResourceType.OPERATION)
        {
            newInstance = createOperation(key, parent);
        }
        else
        {
            newInstance = super.createInstance(type, key, parent);
        }

        if (newInstance == null)
        {
            throw new ResourceException(
                    String
                            .format(
                                    "Unable to create new instance for resourceType=%s for parent of type=%s",
                                    type, parent.getType()));
        }

        newInstance.setDirectoryType(DirectoryType.PHYSICAL);
        return newInstance;

    }

    private static Resource createSite(String key, ResourceNode parent)
            throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.ROOT)
        {
            throw new ResourceException(String.format(
                    "You cannot create a SITE in of type '%s'", parent
                            .getResource().getType()));

        }

        Site site = new Site(key);

        return site;

    }

    private static Resource createCluster(String key, ResourceNode parent)
            throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.SITE)
        {
            throw new ResourceException(String.format(
                    "You cannot create a CLUSTER in directory of type '%s'",
                    parent.getResource().getType()));
        }

        Cluster cluster = new Cluster(key);

        return cluster;

    }

    public static <T> ResourceNode addQueue(String key, ResourceNode parent,
            T type, Directory directory) throws ResourceException
    {
        Queue<T> queue = new Queue<T>(key);

        return parent.addChild(queue);
    }

    private static Resource createProcess(String key, ResourceNode parent)
            throws ResourceException
    {
        if (parent.getResource().getType() != ResourceType.MEMBER)
        {
            throw new ResourceException(String.format(
                    "You cannot create a PROCESS in directory type '%s'",
                    parent.getResource().getType()));
        }

        Process process = new Process(key);

        return process;

    }

    private static Resource createResourceManager(String key,
            ResourceNode parent) throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.PROCESS)
        {
            throw new ResourceException(
                    String
                            .format(
                                    "You cannot create a RESOURCE MANAGER in directory of type '%s'",
                                    parent.getResource().getType()));
        }

        ResourceManager resourceManager = new ResourceManager(key);

        return resourceManager;

    }

    private static Resource createClusterMember(String key, ResourceNode parent)
            throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.CLUSTER)
        {
            throw new ResourceException(String.format(
                    "You cannot create a MEMBER in directory of type '%s'",
                    parent.getResource().getType()));
        }

        Member member = new Member(key);

        return member;

    }

    private static Resource createOperation(String key, ResourceNode parent)
            throws ResourceException
    {

        if (parent.getResource().getType() != ResourceType.RESOURCE_MANAGER)
        {
            throw new ResourceException(String.format(
                    "You cannot create an OPERATION in directory of type '%s'",
                    parent.getResource().getType()));
        }

        Operation operation = new Operation(key);

        return operation;

    }

}
