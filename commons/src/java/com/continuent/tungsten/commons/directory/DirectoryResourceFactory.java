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

package com.continuent.tungsten.commons.directory;

import java.io.Serializable;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.shared.Folder;
import com.continuent.tungsten.commons.cluster.resource.shared.Queue;
import com.continuent.tungsten.commons.cluster.resource.shared.ResourceConfiguration;
import com.continuent.tungsten.commons.exception.ResourceException;
import com.continuent.tungsten.commons.utils.ReflectUtils;

public abstract class DirectoryResourceFactory implements Serializable
{
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new resource instance of the specified type
     * @param type
     * @param key
     * @param parent
     * 
     * @return the instance created
     * @throws ResourceException
     */
    public Resource createInstance(ResourceType type, String key,
            ResourceNode parent)
            throws ResourceException
    {
        Resource newInstance = null;

        if (type == ResourceType.FOLDER)
        {
            newInstance = createFolder(key, parent);
        }
        else if (type == ResourceType.CONFIGURATION)
        {
            newInstance = createResourceConfiguration(key, parent);
        }
        else
        {
            throw new ResourceException(
                    String
                            .format(
                                    "Unable to create new instance for resourceType=%s for parent of type=%s",
                                    type, parent.getType()));
        }

        if (newInstance == null)
        {
            throw new ResourceException(
                    String
                            .format(
                                    "Unable to create new instance for resourceType=%s for parent of type=%s",
                                    type, parent.getType()));
        }

        return newInstance;

    }
    
    
    /**
     * @param type
     * @param key
     * @param parent
     * @return the node created/added
     * @throws ResourceException
     */
    public ResourceNode addInstance(ResourceType type, String key,
            ResourceNode parent)
            throws ResourceException
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
    public Resource copyInstance(ResourceNode source,
            String destinationKey, ResourceNode destination)
            throws ResourceException
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

    public static <T> ResourceNode addQueue(String key,
            ResourceNode parent, T type,
            Directory directory) throws ResourceException
    {
        Queue<T> queue = new Queue<T>(key);
        
        return parent.addChild(queue);
    }

    private static Resource createFolder(String key, ResourceNode parent) throws ResourceException
    {
        Folder folder = new Folder(key);

        return folder;

    }

    private static Resource createResourceConfiguration(String key,
            ResourceNode parent) throws ResourceException
    {
        ResourceConfiguration config = new ResourceConfiguration(key);

        return config;

    }
    
    

   

}
