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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.cluster.resource.physical;

import javax.management.ObjectName;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.directory.ClusterPhysicalDirectory;
import com.continuent.tungsten.commons.directory.ResourceNode;
import com.continuent.tungsten.commons.jmx.DynamicMBeanHelper;
import com.continuent.tungsten.commons.jmx.DynamicMBeanOperation;

@org.jboss.cache.pojo.annotation.Replicable
public class ResourceManager extends Resource
{
    /**
     * 
     */
    private static final long  serialVersionUID = 1L;
    private DynamicMBeanHelper manager          = null;

    public ResourceManager(String name)
    {
        super(ResourceType.RESOURCE_MANAGER, name);
        this.childType = ResourceType.OPERATION;
        this.isContainer = true;
    }

    public void setManager(DynamicMBeanHelper manager, ResourceNode parent,
            ClusterPhysicalDirectory directory, String sessionID) throws Exception
    {
        this.manager = manager;
        addOperations(parent, directory, sessionID);

    }

    private void addOperations(ResourceNode parent, ClusterPhysicalDirectory directory,
            String sessionID) throws Exception
    {
        for (DynamicMBeanOperation mbOperation : manager.getMethods().values())
        {
            ResourceNode operationNode = directory.getResourceFactory()
                    .addInstance(ResourceType.OPERATION, mbOperation.getName(),
                            parent);

            Operation operation = (Operation) operationNode.getResource();
            operation.setOperation(mbOperation);

        }
    }

    public ObjectName getObjectName()
    {
        return manager.getName();
    }
    
    public String toString()
    {
        return getName() + "/";
    }

}
