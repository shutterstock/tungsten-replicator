
package com.continuent.tungsten.commons.directory;

import com.continuent.tungsten.commons.cluster.resource.Resource;

public class ResourceLink extends ResourceNode
{

    public ResourceNode       linkParent           = null;

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     * Creates a new <code>ResourceLink</code> object
     * 
     * @param parent
     * @param resource
     */
    public ResourceLink(ResourceNode parent, Resource resource)
    {
        super(resource);
        this.linkParent = parent;
    }
    
    public ResourceNode getParent()
    {
        return linkParent;
    }

}
