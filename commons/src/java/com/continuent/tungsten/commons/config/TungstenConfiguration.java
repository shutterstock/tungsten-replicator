
package com.continuent.tungsten.commons.config;

import java.util.ArrayList;
import java.util.List;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;

public class TungstenConfiguration
{
    private Resource                    resource;
    private TungstenConfiguration       parent   = null;
    private List<TungstenConfiguration> children = new ArrayList<TungstenConfiguration>();

    public TungstenConfiguration(Resource resource)
    {
        this.resource = resource;
    }

    public void addChild(TungstenConfiguration config)
    {
        children.add(config);
        config.setParent(this);
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getName()
     */
    public String getName()
    {
        return resource.getName();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#getType()
     */
    public ResourceType getType()
    {
        return resource.getType();
    }

    /**
     * @see com.continuent.tungsten.commons.cluster.resource.Resource#toProperties()
     */
    public TungstenProperties toProperties()
    {
        return resource.toProperties();
    }

    /**
     * Returns the resource value.
     * 
     * @return Returns the resource.
     */
    public Resource getResource()
    {
        return resource;
    }

    /**
     * Sets the resource value.
     * 
     * @param resource The resource to set.
     */
    public void setResource(Resource resource)
    {
        this.resource = resource;
    }

    /**
     * Returns the children value.
     * 
     * @return Returns the children.
     */
    public List<TungstenConfiguration> getChildren()
    {
        return children;
    }

    /**
     * Returns the parent value.
     * 
     * @return Returns the parent.
     */
    public TungstenConfiguration getParent()
    {
        return parent;
    }

    /**
     * Sets the parent value.
     * 
     * @param parent The parent to set.
     */
    public void setParent(TungstenConfiguration parent)
    {
        this.parent = parent;
    }

    public String getPath()
    {
        return getPath(this);
    }

    public String getPath(TungstenConfiguration config)
    {
        String path = config.getName();
        TungstenConfiguration parent = null;

        while ((parent = config.getParent()) != null)
        {
            path = parent.getName()
                    + (parent.getName().equals("/") ? path : "/" + path);
            config = parent;
        }

        return path;
    }

    public String toString()
    {
        return getPath(this);
    }

}
