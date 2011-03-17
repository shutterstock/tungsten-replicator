
package com.continuent.tungsten.commons.directory;

import java.io.Serializable;

@org.jboss.cache.pojo.annotation.Replicable
public class DirectorySession implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    DirectorySessionManager   parent           = null;
    String                    sessionID        = null;

    /**
     * Stores the current node for this session, for each view
     */
    ResourceNode[]            currentNodeArray = new ResourceNode[DirectoryType
                                                       .values().length];
    private long              timeCreated      = 0L;
    private long              lastTimeAccessed = 0L;

    public DirectorySession(DirectorySessionManager parent, String sessionID,
            DirectoryType type)
    {
        this.parent = parent;
        this.sessionID = sessionID;
        this.currentNodeArray[type.getIndex()] = null;
        this.timeCreated = System.currentTimeMillis();
        this.lastTimeAccessed = this.timeCreated;
    }

    /**
     * @return the parent
     */
    public DirectorySessionManager getParent()
    {
        return parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(DirectorySessionManager parent)
    {
        this.parent = parent;
    }

    /**
     * @return the sessionID
     */
    public String getSessionID()
    {
        return sessionID;
    }

    /**
     * @param sessionID the sessionID to set
     */
    public void setSessionID(String sessionID)
    {
        this.sessionID = sessionID;
    }

    /**
     * @return the currentNode
     */
    public ResourceNode getCurrentNode(DirectoryType type)
    {
        return currentNodeArray[type.getIndex()];
    }

    /**
     * @param currentNode the currentNode to set
     */
    public void setCurrentNode(DirectoryType type, ResourceNode currentNode)
    {
        this.currentNodeArray[type.getIndex()] = currentNode;
    }

    public String toString()
    {
        return "sessionID=" + getSessionID();
    }

    public long getTimeCreated()
    {
        return timeCreated;
    }

    public void setTimeCreated(long timeCreated)
    {
        this.timeCreated = timeCreated;
    }

    public long getLastTimeAccessed()
    {
        return lastTimeAccessed;
    }

    public void setLastTimeAccessed(long lastTimeAccessed)
    {
        this.lastTimeAccessed = lastTimeAccessed;
    }

}
