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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.commons.cluster.resource.physical;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.jmx.DynamicMBeanOperation;

public class Operation extends Resource 
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    DynamicMBeanOperation operation = null;

    public Operation()
    {
        super(ResourceType.OPERATION, "UNKNOWN");
    }

    public Operation(String name)
    {
        super(ResourceType.OPERATION, name);
        init();
    }

    private void init()
    {
        this.childType = ResourceType.NONE;
        this.isContainer = false;
    }

    public void setOperation(DynamicMBeanOperation operation)
    {
        this.operation = operation;
    }

    /**
     * @return the operation
     */
    public DynamicMBeanOperation getOperation()
    {
        return operation;
    }
    
    @Override
    public String toString()
    {
        if (operation != null)
            return operation.toString();
        else
            return name;
        
    }
    
    public String describe(boolean detailed)
    {
        StringBuilder builder = new StringBuilder();
        
        if (operation != null)
        {
            builder.append(String.format("name=%s, type=%s\n", getName(), getType()));
            builder.append(("{\n"));
            builder.append(String.format("  description=%s\n", operation.getDescription()));
            builder.append(String.format("  usage=%s\n", operation.getUsage()));
            String paramDesc = operation.getParamDescription(true, "    ");
            if (paramDesc != null && paramDesc.length() > 0)
            {
                builder.append(paramDesc);
                builder.append("\n");
            }
            builder.append(("}"));
            
            return builder.toString();
        }
        
       return super.describe(detailed);
    }
    
}
