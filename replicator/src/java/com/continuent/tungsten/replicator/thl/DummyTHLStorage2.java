/**
// * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class implements a mock THL storage driver that can be used for unit
 * testing. Storage contents are normally dropped from memory once reference
 * counts of users to go zero. You can make particular storage databases persist
 * using the static call {@link #setPersistence(String, boolean)}.
 */
public class DummyTHLStorage2 implements THLStorage
{
    private static Logger logger = Logger.getLogger(DummyTHLStorage2.class);
    private String        dbName = null;

    // Composite event key consisting of sequence number + fragment number.
    class EventKey implements Comparable<EventKey>
    {
        final long  seqno;
        final short fragno;

        EventKey(long seqno, short fragno)
        {
            this.seqno = seqno;
            this.fragno = fragno;
        }

        /** Compare based on seqno followed by fragment. */
        public int compareTo(EventKey other)
        {
            if (other == null)
                return 0;
            else if (seqno < other.seqno)
                return -1;
            else if (seqno > other.seqno)
                return 1;
            else if (fragno < other.fragno)
                return -1;
            else if (fragno > other.fragno)
                return 1;
            else
                return 0;
        }

        /** Determine equality. */
        public boolean equals(Object o)
        {
            if (o == null)
                return false;
            else if (o instanceof EventKey)
                return compareTo((EventKey) o) == 0;
            else
                return false;
        }

        /**
         * Compute hashcode using seqno and fragno where seqno becomes upper two
         * bytes. This will produce weird but consistent results for seqno
         * values > 32K.
         */
        public int hashCode()
        {
            short shortSeqno = (short) (seqno & 0xFFFF);
            int upperBytes = shortSeqno << 16;
            int hashCode = upperBytes | fragno;
            return hashCode;
        }
    }

    // In-memory storage.
    class Storage
    {
        Map<EventKey, String>   idMap    = null;
        Map<EventKey, THLEvent> eventMap = null;
        int                     refcnt   = 0;

        public Storage()
        {
            idMap = new HashMap<EventKey, String>();
            eventMap = new HashMap<EventKey, THLEvent>();
            refcnt = 0;
        }

        public Map<EventKey, String> getIdMap() throws THLException
        {
            if (refcnt <= 0)
                throw new THLException("refcnt");
            return idMap;
        }

        public Map<EventKey, THLEvent> getEventMap() throws THLException
        {
            if (refcnt <= 0)
                throw new THLException("refcnt");
            return eventMap;
        }

        public void ref()
        {
            refcnt++;
        }

        public void unref(boolean isPersistent)
        {
            refcnt--;
            if (refcnt == 0 && !isPersistent)
            {
                idMap.clear();
                idMap = null;
                eventMap.clear();
                eventMap = null;
            }
        }

        public int getRefcnt()
        {
            return refcnt;
        }
    }

    static Map<String, Storage> instances      = new HashMap<String, Storage>();
    static Map<String, Boolean> persistenceMap = new HashMap<String, Boolean>();

    /** Allows clients to make certain storage persistent. */
    static void setPersistence(String dbName, boolean isPersistent)
            throws THLException
    {
        persistenceMap.put(dbName, isPersistent);

        // Clean up unreferenced storage.
        if (!isPersistent)
        {
            try
            {
                Storage s = getStorage(dbName);
                if (s.getRefcnt() == 0)
                {
                    logger.info("unref " + dbName);
                    instances.remove(dbName);
                }
            }
            catch (THLException e)
            {
                // don't bother; storage does not exist
            }
        }
    }

    /** Retrieve named storage. */
    public static Storage getStorage(String name) throws THLException
    {
        synchronized (instances)
        {
            Storage ret = instances.get(name);
            if (ret == null)
                throw new THLException("Storage " + name + " not found");
            return ret;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setPassword(java.lang.String)
     */
    public void setPassword(String password)
    {
        // Ignored.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUrl(java.lang.String)
     */
    public void setUrl(String url)
    {
        // Ignored.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setUser(java.lang.String)
     */
    public void setUser(String user)
    {
        // Ignored.
    }

    public THLEvent find(long seqno) throws THLException
    {
        EventKey key = new EventKey(seqno, (short) 0);
        synchronized (instances)
        {
            /* THLEvent is always assumed to be non-null */
            return getStorage(dbName).getEventMap().get(key);
        }
    }

    public String getEventId(long seqno) throws THLException
    {
        EventKey key = new EventKey(seqno, (short) 0);
        synchronized (instances)
        {
            return getStorage(dbName).getIdMap().get(key);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxEventId(String)
     */
    public String getMaxEventId(String sourceId) throws THLException
    {
        synchronized (instances)
        {
            EventKey key = new EventKey(getMaxSeqno(), (short) 0);
            return getStorage(dbName).getIdMap().get(key);
        }
    }

    /** Return the highest sequence number by scanning the list. */
    public long getMaxSeqno() throws THLException
    {
        synchronized (instances)
        {
            Map<EventKey, String> idMap = getStorage(dbName).getIdMap();
            if (idMap.isEmpty())
                return -1;
            long maxValue = -1;
            for (EventKey ek : idMap.keySet())
            {
                maxValue = ek.seqno > maxValue ? ek.seqno : maxValue;
            }
            return maxValue;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMaxCompletedSeqno()
     */
    public long getMaxCompletedSeqno() throws THLException
    {
        // FIXME: This is not adequate, check also status
        return getMaxSeqno();
    }

    /** Return the lowest sequence number by scanning the list. */
    public long getMinSeqno() throws THLException
    {
        synchronized (instances)
        {
            Map<EventKey, String> idMap = getStorage(dbName).getIdMap();
            if (idMap.isEmpty())
                return -1;
            long minValue = Long.MAX_VALUE;
            for (EventKey ek : idMap.keySet())
            {
                minValue = ek.seqno < minValue ? ek.seqno : minValue;
            }
            return minValue;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#store(com.continuent.tungsten.replicator.thl.THLEvent,
     *      boolean)
     */
    public void store(THLEvent event, boolean doCommit, boolean syncTHL)
            throws THLException
    {
        synchronized (instances)
        {
            Map<EventKey, String> idMap = getStorage(dbName).getIdMap();
            Map<EventKey, THLEvent> eventMap = getStorage(dbName).getEventMap();
            EventKey key = new EventKey(event.getSeqno(), event.getFragno());
            if (idMap.containsKey(key))
            {
                throw new THLException("Duplicate entry");
            }
            if (eventMap.containsKey(key))
            {
                throw new THLException("Duplicate entry");
            }
            idMap.put(key, event.getEventId());
            eventMap.put(key, event);
            instances.notifyAll();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws THLException
    {
        dbName = context.getReplicatorSchemaName();

        if (dbName == null)
        {
            throw new THLException("uri not specified");
        }
        synchronized (instances)
        {
            Storage s = instances.get(dbName);
            if (s == null)
            {
                s = new Storage();
                instances.put(dbName, s);
            }
            s.ref();
            logger.info("ref " + dbName);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release() throws THLException
    {
        synchronized (instances)
        {
            Storage s = getStorage(dbName);
            boolean isPersistent = false;
            if (persistenceMap.get(dbName) != null)
                isPersistent = persistenceMap.get(dbName);
            s.unref(isPersistent);
            if (s.getRefcnt() == 0 && !isPersistent)
            {
                logger.info("unref " + dbName);
                instances.remove(dbName);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#find(long, short)
     */
    public THLEvent find(long seqno, short fragno) throws THLException,
            InterruptedException
    {
        EventKey key = new EventKey(seqno, fragno);
        THLEvent event = null;
        synchronized (instances)
        {
            // THLEvent is always assumed to be non-null; if we don't find it
            // there may be a pending commit on a fragment.
            Map<EventKey, THLEvent> eventMap = getStorage(dbName).getEventMap();
            if (fragno == 0)
                event = eventMap.get(key);
            else
            {
                while ((event = eventMap.get(key)) == null)
                {
                    // Wait at most 5 seconds.
                    instances.wait(5000);
                }
            }
            return event;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#setStatus(long,
     *      short, short, java.lang.String)
     */
    public void setStatus(long seqno, short fragno, short status, String msg)
            throws THLException
    {
        synchronized (instances)
        {
            Map<EventKey, THLEvent> eventMap = getStorage(dbName).getEventMap();
            EventKey key = new EventKey(seqno, fragno);

            THLEvent event = eventMap.get(key);
            if (event != null)
            {
                if (event.getStatus() != status)
                    logger.debug("Changing event status " + event.getStatus()
                            + " -> " + status);
                event.setStatus(status, msg);
            }
            else
            {
                throw new THLException("Event not found");
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#updateFailedStatus(com.continuent.tungsten.replicator.thl.THLEventStatus,
     *      java.util.ArrayList)
     */
    public void updateFailedStatus(THLEventStatus failedEvent,
            ArrayList<THLEventStatus> events)
    {
        // not supported.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#updateSuccessStatus(java.util.ArrayList,
     *      java.util.ArrayList)
     */
    public void updateSuccessStatus(ArrayList<THLEventStatus> succeededEvents,
            ArrayList<THLEventStatus> skippedEvents) throws THLException
    {
        // not support.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getMinMaxSeqno()
     */
    public long[] getMinMaxSeqno() throws THLException
    {
        return new long[]{getMinSeqno(), getMaxSeqno()};
    }

    public short getMaxFragno(long seqno) throws THLException
    {
        return 0;
    }

    public THLBinaryEvent findBinaryEvent(long seqno, short fragno)
            throws THLException
    {
        throw new THLException("Not yet implemented");
    }

    public int delete(Long low, Long high, String before) throws THLException,
            InterruptedException
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLStorage#getLastAppliedEvent()
     */
    public ReplDBMSHeader getLastAppliedEvent()
    {
        return null;
    }
}
