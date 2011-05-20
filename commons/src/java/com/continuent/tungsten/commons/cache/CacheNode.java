/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2011 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.cache;

/**
 * Node for a single entry in an indexed LRU cache. The node contains threaded
 * references to earlier and later nodes in the LRU list.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class CacheNode<T>
{

    // Value we are storing.
    private String       key;
    private long         lastAccessMillis;
    private T            value;

    // Previous and after nodes in the LRU list.
    private CacheNode<T> before;
    private CacheNode<T> after;

    /** Create node and set initial access time. */
    public CacheNode(String key, T value)
    {
        this.key = key;
        this.value = value;
        this.lastAccessMillis = System.currentTimeMillis();
    }

    /**
     * Release resources associated with the value. Must be overridden by
     * clients to implement type-specific resource management. The node is
     * unusable after this call.
     */
    public void release()
    {
        value = null;
    }

    /* Returns the key to this node. */
    public String getKey()
    {
        return key;
    }

    /** Return the node value. */
    public T get()
    {
        lastAccessMillis = System.currentTimeMillis();
        return value;
    }

    /** Returns time of last access. */
    public long getLastAccessMillis()
    {
        return lastAccessMillis;
    }

    /** Return the before (newer) node or null in LRU list. */
    public CacheNode<T> getBefore()
    {
        return before;
    }

    /** Set the before node in the LRU list. */
    public void setBefore(CacheNode<T> previous)
    {
        this.before = previous;
    }

    /** Return the after (older) node in the LRU list. */
    public CacheNode<T> getAfter()
    {
        return after;
    }

    /** Set the after node in the LRU list. */
    public void setAfter(CacheNode<T> next)
    {
        this.after = next;
    }
}