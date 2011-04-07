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

package com.continuent.tungsten.commons.directory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;

/**
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
@org.jboss.cache.pojo.annotation.Replicable
public class ResourceNode implements Serializable
{
    /**
     *
     */
    private static final long        serialVersionUID = 1L;

    private Resource                 resource;
    private boolean                  link             = false;

    public Map<String, ResourceNode> children         = new HashMap<String, ResourceNode>();
    public Vector<ResourceNode>      links            = new Vector<ResourceNode>();
    public ResourceNode              parent;

    /**
     * @param resource
     */
    public ResourceNode(Resource resource)
    {
        this.resource = resource;
    }

    /**
     * @return the resource held by this node
     */
    public Resource getResource()
    {
        return resource;
    }

    /**
     * @return the type of this node
     */
    public ResourceType getType()
    {
        return resource.getType();
    }

    /**
     * @return the allowed type for children of this node
     */
    public ResourceType getChildType()
    {
        return resource.getChildType();
    }

    /**
     * @return the key for this node
     */
    public String getKey()
    {
        return resource.getKey();
    }

    /**
     * @return true if this node can contain other resources, otherwise false
     */
    public boolean isContainer()
    {
        return resource.isContainer();
    }

    /**
     * @return true if the resource for this node is executable, otherwise false
     */
    public boolean isExecutable()
    {
        return resource.isExecutable();
    }

    /**
     * Return a map of the current children of this node or an empty map.
     *
     * @return the children of Node<String,RouterResource>
     */
    public synchronized Map<String, ResourceNode> getChildren()
    {
        if (this.children == null)
        {
            return new LinkedHashMap<String, ResourceNode>();
        }
        return this.children;
    }

    /**
     * @param children
     */
    public synchronized void setChildren(Map<String, ResourceNode> children)
    {
        this.children = children;
    }

    /**
     * Returns the number of immediate children of this
     * Node<String,RouterResource>.
     *
     * @return the number of immediate children.
     */
    public synchronized int getNumberOfChildren()
    {
        if (children == null)
        {
            return 0;
        }
        return children.size();
    }

    /**
     * Adds a child node to this node
     *
     * @param childNode
     */
    public synchronized void addChild(ResourceNode childNode)
    {
        childNode.parent = this;
        children.put(childNode.getKey(), childNode);
    }

    /**
     * Adds a child node to this node
     *
     * @param nodeToLink
     */
    public synchronized void link(ResourceNode nodeToLink)
    {
        ResourceLink link = new ResourceLink(this, nodeToLink.getResource());
        children.put(link.getKey(), link);
        nodeToLink.addReference(this);
    }

    /**
     * Removes a child node from this node
     */
    public synchronized void unlink()
    {
        for (ResourceNode referencingNode : getLinks())
        {
            referencingNode.removeChild(this.getKey());
        }
    }

    /**
     * Points a links back at a node that references it as a link.
     *
     * @param referencingNode
     */
    public synchronized void addReference(ResourceNode referencingNode)
    {
        this.getLinks().add(referencingNode);
    }

    /**
     * Utility method that creates a new resource node from the resource passed
     * in and then adds it as a child.
     *
     * @param child
     * @return the new node created/added as a result of this method
     */
    public synchronized ResourceNode addChild(Resource child)
    {
        ResourceNode childNode = new ResourceNode(child);

        childNode.parent = this;
        children.put(child.getKey(), childNode);

        return childNode;
    }

    /**
     * Remove a child by key
     *
     * @param key
     */
    public synchronized void removeChild(String key)
    {
        ResourceNode childToRemove = children.get(key);

        for (ResourceNode link : childToRemove.getLinks())
        {
            link.removeChild(key);
        }

        children.remove(key);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getKey());

        sb.append(" {").append(getResource().toString()).append(",[");
        int i = 0;
        for (ResourceNode e : getChildren().values())
        {
            if (i > 0)
            {
                sb.append(",");
            }
            sb.append(e.getResource().toString());
            i++;
        }
        sb.append("]").append("} ");
        return sb.toString();
    }

    public ResourceNode getParent()
    {
        return this.parent;
    }

    public void setParent(ResourceNode parent)
    {
        this.parent = parent;
    }

    public void setResource(Resource resource)
    {
        this.resource = resource;
    }

    /**
     * Returns the links value.
     *
     * @return Returns the links.
     */
    public Vector<ResourceNode> getLinks()
    {
        return links;
    }

    /**
     * Returns the link value.
     *
     * @return Returns the link.
     */
    public boolean isLink()
    {
        return link;
    }

    /**
     * Sets the link value.
     *
     * @param link The link to set.
     */
    public void setLink(boolean link)
    {
        this.link = link;
    }
}
