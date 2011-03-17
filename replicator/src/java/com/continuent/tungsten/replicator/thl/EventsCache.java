/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009-2010 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * Implements a simple hash map to hold events. If the cache is full we age out
 * old items in FIFO order.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class EventsCache
{
    static Logger                         logger    = Logger
                                                            .getLogger(EventsCache.class);
    private int                           cacheSize = 0;
    private LinkedBlockingQueue<THLEvent> fifo;
    private HashMap<Long, THLEvent>       cache;

    public EventsCache(int cacheSize)
    {
        this.cacheSize = cacheSize;
        if (cacheSize > 0)
        {
            logger.info("Allocating THL event cache; size=" + cacheSize);
            this.fifo = new LinkedBlockingQueue<THLEvent>(cacheSize);
            this.cache = new HashMap<Long, THLEvent>(cacheSize);
        }
    }

    public boolean isEmpty()
    {
        return (cacheSize <= 0 || cache.isEmpty());
    }

    /**
     * Add an event to the cache, clearing space if necessary.
     */
    public synchronized void put(THLEvent thlEvent) throws InterruptedException
    {
        // If cache is suppressed do nothing.
        if (cacheSize > 0)
        {
            // Clear space.
            while (cache.size() >= cacheSize)
            {
                THLEvent old = fifo.remove();
                cache.remove(old.getSeqno());
            }

            if (thlEvent.getFragno() == 0 && thlEvent.getLastFrag())
            {
                // This event is not fragmented, so just cache it
                fifo.put(thlEvent);
                cache.put(thlEvent.getSeqno(), thlEvent);
            }
            // else fragmented events are not cached as this could bring OOM
            // issues
        }
    }

    /**
     * Look up and return the cached item, if found.
     */
    public synchronized THLEvent get(long seqno)
    {
        if (cacheSize > 0)
            return cache.get(seqno);
        else
            return null;
    }
}
