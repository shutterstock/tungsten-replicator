/**
 * Tungsten Scale-Out Stack
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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.physical.Replicator;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.storage.Store;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a standard Store interface on the THL (transaction history log).
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class THL implements Store
{
    protected static Logger               logger              = Logger.getLogger(THL.class);

    // Version information and constants.
    public static final int               MAJOR               = 1;
    public static final int               MINOR               = 3;
    public static final String            SUFFIX              = "";
    public static final String            URI_SCHEME          = "thl";

    // Name of this store.
    private String                        name;

    // Store properties.
    private String                        storageListenerUri  = "thl://0.0.0.0:2112/";
    protected String                      storage             = JdbcTHLStorage.class
                                                                      .getName();
    protected String                      url;
    protected String                      user;
    protected String                      password;
    private int                           cacheSize           = 0;
    private int                           resetPeriod         = 1;

    // Storage management variables.
    protected PluginContext               context;
    protected Hashtable<Long, THLStorage> storageHandlers;
    protected THLStorage                  adminStorageHandler = null;
    private AtomicCounter                 sequencer;

    // Cache management values.
    private EventsCache                   eventsCache         = null;
    private boolean                       useCache;

    // Storage connectivity.
    private Server                        server              = null;

    /** Creates a store instance. */
    public THL()
    {
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    // Accessors for configuration.
    public String getStorageListenerUri()
    {
        return storageListenerUri;
    }

    public void setStorageListenerUri(String storageListenerUri)
    {
        this.storageListenerUri = storageListenerUri;
    }

    public String getStorage()
    {
        return storage;
    }

    protected THLStorage getStorageHandler() throws ReplicatorException,
            InterruptedException
    {
        THLStorage storage = null;
        Long threadId = Long.valueOf(Thread.currentThread().getId());
        if (logger.isDebugEnabled())
            logger.debug("Looking for storage handler for "
                    + Thread.currentThread().getName() + " (id :" + threadId
                    + ")");
        if (storageHandlers != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("List of currently opened storage handlers : "
                        + storageHandlers);
            if (storageHandlers.containsKey(threadId))
            {
                storage = storageHandlers.get(threadId);
                if (logger.isDebugEnabled())
                    logger.debug("Retrieved storage handler "
                            + storage.toString() + " for "
                            + Thread.currentThread().getName());
            }
            else
            {
                storage = getThlStorageHandler();
                if (logger.isDebugEnabled())
                    logger.debug("Using new storage handler "
                            + storage.toString() + " for "
                            + Thread.currentThread().getName());
                storageHandlers.put(threadId, storage);
            }
        }
        else
        {
            storageHandlers = new Hashtable<Long, THLStorage>();
            storage = getThlStorageHandler();
            if (logger.isDebugEnabled())
                logger.debug("Using new storage handler " + storage + " for "
                        + Thread.currentThread().getName());
            storageHandlers.put(threadId, storage);
        }
        return storage;
    }

    public void setStorage(String storageAccessor)
    {
        this.storage = storageAccessor;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public int getCacheSize()
    {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize)
    {
        this.cacheSize = cacheSize;
    }

    public int getResetPeriod()
    {
        return resetPeriod;
    }

    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    // STORE API STARTS HERE.

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.storage.Store#getMaxStoredSeqno(boolean)
     */
    public long getMaxStoredSeqno(boolean adminCommand)
    {
        // Choose the right storage to be used
        THLStorage storage = null;
        if (adminCommand)
        {
            storage = adminStorageHandler;
        }
        else
        {
            try
            {
                storage = getStorageHandler();
            }
            catch (ReplicatorException e)
            {
                logger.error(
                        "Unable to fetch maximum sequence number from THL", e);
                return -1;
            }
            catch (InterruptedException e)
            {
                logger.error(
                        "Unable to fetch maximum sequence number from THL", e);
                return -1;
            }
        }

        // And query the chosen storage handler
        try
        {
            return storage.getMaxSeqno();
        }
        catch (THLException e)
        {
            logger.error("Unable to fetch maximum sequence number from THL", e);
            return -1;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.storage.Store#getMinStoredSeqno(boolean)
     */
    public long getMinStoredSeqno(boolean adminCommand)
    {
        // Choose the right storage to be used
        THLStorage storage = null;
        if (adminCommand)
        {
            storage = adminStorageHandler;
        }
        else
        {
            try
            {
                storage = getStorageHandler();
            }
            catch (ReplicatorException e)
            {
                logger.error(
                        "Unable to fetch minimum sequence number from THL", e);
                return -1;
            }
            catch (InterruptedException e)
            {
                logger.error(
                        "Unable to fetch minimum sequence number from THL", e);
                return -1;
            }
        }

        // And query the chosen storage handler
        try
        {
            return storage.getMinSeqno();
        }
        catch (THLException e)
        {
            logger.error("Unable to fetch minimum sequence number from THL", e);
            return -1;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Store variables.
        this.context = context;

        storageHandlers = new Hashtable<Long, THLStorage>();
        // Create an administrative connection (to be used by admin commands, as
        // status(), for example)
        this.adminStorageHandler = this.getThlStorageHandler();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Configure cache, if used.
        useCache = (cacheSize > 0);
        if (cacheSize > 0)
        {
            useCache = true;
            eventsCache = new EventsCache(cacheSize);
            context.getMonitor().setCacheSize(cacheSize);
        }
        else
            useCache = false;

        // Set sequencer.
        sequencer = new AtomicCounter(getStorageHandler().getMaxSeqno());

        // Start server for THL connections.
        if (context.isRemoteService() == false)
        {
            try
            {
                server = new Server(context, sequencer, this);
                server.start();
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to start THL server", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public synchronized void release(PluginContext context)
            throws ReplicatorException
    {
        // Cancel server.
        if (server != null)
        {
            try
            {
                server.stop();
            }
            catch (InterruptedException e)
            {
                logger.warn(
                        "Server stop operation was unexpectedly interrupted", e);
            }
            finally
            {
                server = null;
            }
        }

        // Drop storage.
        if (adminStorageHandler != null)
        {
            releaseThlStorageHandler(adminStorageHandler);
            adminStorageHandler = null;
        }
        // Release all storage handlers
        if (storageHandlers != null)
            synchronized (storageHandlers)
            {
                for (Iterator<THLStorage> iterator = storageHandlers.values()
                        .iterator(); iterator.hasNext();)
                {
                    releaseThlStorageHandler(iterator.next());
                    iterator.remove();
                }
            }
    }

    // STORE ADAPTER API BEGINS HERE

    /**
     * Store event in THL.
     *
     * @param replEvent Event to store
     * @param doCommit If true, commit this and previous uncommitted events
     * @param syncTHL If true, sync to THL table to track commits
     * @throws ReplicatorException Thrown if there is a storage error
     * @throws InterruptedException Thrown if we are cancelled during operation
     */
    public void storeEvent(ReplDBMSEvent replEvent, boolean doCommit,
            boolean syncTHL) throws ReplicatorException, InterruptedException
    {
        // TODO: Compute a checksum if desired.
        //
        // if (context.isDoChecksum())
        // replEvent.addChecksum();

        // Store event.
        THLEvent thlEvent = new THLEvent(replEvent.getEventId(), replEvent);
        try
        {
            getStorageHandler().store(thlEvent, doCommit, syncTHL);
            logger.debug("Stored event " + replEvent.getSeqno());
        }
        catch (THLException e)
        {
            throw new ReplicatorException("Unable to store event: seqno="
                    + replEvent.getSeqno(), e);
        }

        // Add thlEvent to cache if active.
        if (useCache)
        {
            eventsCache.put(thlEvent);
        }

        if (replEvent.getLastFrag())
            // Update the sequence number.
            sequencer.setSeqno(replEvent.getSeqno());
    }

    /**
     * Fetches an event from the THL
     *
     * @param seqno Sequence number of the event
     * @param fragno Fragment number
     * @return Requested event or null if it should be skipped.
     * @throws ReplicatorException Thrown if there is a storage error
     * @throws InterruptedException Thrown if we are cancelled during operation
     */
    public ReplDBMSEvent fetchEvent(long seqno, short fragno,
            boolean ignoreSkippedEvents) throws InterruptedException,
            ReplicatorException
    {
        ReplEvent replEvent = null;
        ReplDBMSEvent replDbmsEvent = null;
        THLEvent thlEvent = null;
        long eventSeqno = -1;

        // Ensure sequence number is available.
        sequencer.waitSeqnoGreaterEqual(seqno);

        // Try using the cache.
        if (useCache)
        {
            thlEvent = eventsCache.get(seqno);
            if (thlEvent != null)
                eventSeqno = thlEvent.getSeqno();
        }
        if (eventSeqno == seqno)
        {
            // Cache hit
            if (logger.isDebugEnabled())
                logger.debug("Cache hit for seqno " + seqno);
        }
        else
        {
            THLStorage storage = getStorageHandler();
            thlEvent = storage.find(seqno, fragno);

            if (thlEvent == null)
            {
                logger.debug("Storage range: [" + storage.getMinSeqno() + ","
                        + storage.getMaxSeqno() + "]");
                throw new THLException("Event " + seqno
                        + " missing from storage");
            }
        }
        replEvent = thlEvent.getReplEvent();

        if (replEvent instanceof ReplDBMSEvent)
        {
            replDbmsEvent = (ReplDBMSEvent) replEvent;

            short status = thlEvent.getStatus();
            if (ignoreSkippedEvents
                    && (status == THLEvent.SKIP || status == THLEvent.SKIPPED))
            {
                /*
                 * This event has been marked to be skipped or has already been
                 * skipped
                 */
                return new SkippedEvent(replDbmsEvent.getSeqno());
            }

            // TODO: Implement proper checksums.
            // if (context.isDoChecksum() && !replDbmsEvent.validateChecksum())
            // {
            // throw new ReplicatorException(
            // "Event checksum failed for event: "
            // + replDbmsEvent.getSeqno());
            // }
        }
        return replDbmsEvent;
    }

    /**
     * Returns true if the indicated sequence number is available.
     */
    public boolean pollSeqno(long seqno)
    {
        return seqno <= sequencer.getSeqno();
    }

    /**
     * Returns the maximum event ID in this store.
     */
    public String getMaxEventId() throws ReplicatorException
    {
        try
        {
            return getStorageHandler().getMaxEventId(context.getSourceId());
        }
        catch (THLException e)
        {
            throw new ReplicatorException("Unable to fetch maximum event ID", e);
        }
        catch (InterruptedException e)
        {
            throw new ReplicatorException("Unable to fetch maximum event ID", e);
        }
    }

    /**
     * Create new THLStorage handler.
     *
     * @return THLStorage handler
     * @throws ReplicatorException
     */
    public synchronized THLStorage getThlStorageHandler()
            throws ReplicatorException, InterruptedException
    {
        logger.debug("Configuring THL storage handler: name=" + storage);
        try
        {
            THLStorage storageHandler = (THLStorage) Class.forName(storage)
                    .newInstance();
            storageHandler.setUrl(url);
            storageHandler.setUser(user);
            storageHandler.setPassword(password);
            storageHandler.prepare(context);
            return storageHandler;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to instantiate storage handler: " + storage, e);
        }
    }

    /**
     * Release storage handler.
     *
     * @param storageHandler Handler to be released
     */
    private void releaseThlStorageHandler(THLStorage storageHandler)
    {
        if (storageHandler == null)
            return;

        try
        {
            storageHandler.release();
        }
        catch (ReplicatorException e)
        {
            logger.warn("Error while releasing storage handler", e);
        }
        catch (InterruptedException e)
        {
            logger.warn("Error while releasing storage handler", e);
        }
    }

    /**
     * Get sequence number of the last event which has been processed. Event is
     * taken to be processed if it has reached state COMPLETED or SKIPPED.
     *
     * @return Sequence number of the last event which has been processed
     * @throws THLException
     */
    public long getMaxCompletedSeqno(THLStorage storageHandler)
            throws THLException
    {
        return storageHandler.getMaxCompletedSeqno();
    }

    public short getMaxFragno(long seqno)
    {
        try
        {
            return getStorageHandler().getMaxFragno(seqno);
        }
        catch (THLException e)
        {
            logger.error("Unable to fetch last fragno for seqno " + seqno
                    + " from THL", e);
            return -1;
        }
        catch (ReplicatorException e)
        {
            logger.error("Unable to fetch last fragno for seqno " + seqno
                    + " from THL", e);
            return -1;
        }
        catch (InterruptedException e)
        {
            logger.error("Unable to fetch last fragno for seqno " + seqno
                    + " from THL", e);
            return -1;
        }
    }

    public THLEvent find(long seqno, short fragno) throws THLException,
            InterruptedException
    {
        try
        {
            return getStorageHandler().find(seqno, fragno);
        }
        catch (ReplicatorException e)
        {
            throw new THLException("Unable to find event " + seqno + " / "
                    + fragno + " from storage", e);
        }
    }

    protected void releaseStorageHandler()
    {
        Long threadId = Long.valueOf(Thread.currentThread().getId());
        if (storageHandlers != null)
        {
            synchronized (storageHandlers)
            {
                THLStorage storageHandler = storageHandlers.remove(threadId);
                releaseThlStorageHandler(storageHandler);
            }
        }
    }

    public THLBinaryEvent findBinaryEvent(long seqno, short fragno)
            throws InterruptedException, THLException
    {
        try
        {
            return getStorageHandler().findBinaryEvent(seqno, fragno);
        }
        catch (ReplicatorException e)
        {
            throw new THLException("Unable to find event " + seqno + " / "
                    + fragno + " from storage", e);
        }
    }

    /**
     * Return the last applied event as stored in the CommitSeqnoTable.
     *
     * @return the last applied event or null if nothing was found
     * @throws ReplicatorException
     */
    public ReplDBMSHeader getLastAppliedEvent() throws ReplicatorException
    {
        try
        {
            return getStorageHandler().getLastAppliedEvent();
        }
        catch (ReplicatorException e)
        {
            throw new ReplicatorException("Unable to get last applied event", e);
        }
        catch (InterruptedException e)
        {
            throw new ReplicatorException("Unable to get last applied event", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.storage.Store#status()
     */
    @Override
    public TungstenProperties status()
    {
        TungstenProperties props = new TungstenProperties();
        props.setLong(Replicator.MIN_STORED_SEQNO, getMinStoredSeqno(true));
        props.setLong(Replicator.MAX_STORED_SEQNO, getMaxStoredSeqno(true));
        return props;
    }

    @Override
    public long getMaxCommittedSeqno() throws ReplicatorException
    {
        return getLastAppliedEvent().getSeqno();
    }
}
