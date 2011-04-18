/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.util.Hashtable;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.enterprise.replicator.thl.serializer.ProtobufSerializer;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.THL;
import com.continuent.tungsten.replicator.thl.THLStorage;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class EnterpriseTHL extends THL
{
    private String  logDir                 = "/opt/continuent/logs/";
    private int     logFileSize            = 50000000;                          // 50mb
    private boolean doChecksum             = true;
    private String  eventSerializer        = ProtobufSerializer.class.getName();
    private String  logFileRetention       = "0";
    private int     logConnectionTimeout   = 600;

    // Set this to false for disk storage.
    private boolean useMultiStorageHandler = true;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THL#getThlStorageHandler()
     */
    @Override
    public THLStorage getThlStorageHandler() throws ReplicatorException,
            InterruptedException
    {
        logger.debug("Configuring THL storage handler: name=" + storage);
        try
        {
            THLStorage storageHandler = (THLStorage) Class.forName(storage)
                    .newInstance();
            storageHandler.setUrl(url);
            storageHandler.setUser(user);
            storageHandler.setPassword(password);
            if (storageHandler instanceof DiskTHLStorage)
            {
                DiskTHLStorage handler = (DiskTHLStorage) storageHandler;
                handler.setLogDir(this.logDir);
                handler.setLogFileSize(this.logFileSize);
                handler.setDoChecksum(this.doChecksum);
                handler.setEventSerializer(this.eventSerializer);
                handler.setLogFileRetention(this.logFileRetention);
                handler.setLogConnectionTimeout(this.logConnectionTimeout);
                // THL always require a write connection to the log
                handler.setReadOnly(false);
            }
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
     * Sets the logDir value.
     * 
     * @param logDir The logDir to set.
     */
    public void setLogDir(String logDir)
    {
        this.logDir = logDir;
    }

    /**
     * Sets the logFileSize value in bytes.
     * 
     * @param logFileSize The logFileSize to set.
     */
    public void setLogFileSize(int logFileSize)
    {
        this.logFileSize = logFileSize;
    }

    /**
     * Determines whether to checksum log records.
     * 
     * @param If true use checksums
     */
    public void setDoChecksum(boolean doChecksum)
    {
        this.doChecksum = doChecksum;
    }

    /**
     * Sets the event serializer name.
     */
    public void setEventSerializer(String eventSerializer)
    {
        this.eventSerializer = eventSerializer;
    }

    /**
     * Sets the log file retention interval.
     */
    public void setLogFileRetention(String logFileRetention)
    {
        this.logFileRetention = logFileRetention;
    }

    /**
     * Sets the idle log connection timeout in seconds.
     */
    public void setLogConnectionTimeout(int logConnectionTimeout)
    {
        this.logConnectionTimeout = logConnectionTimeout;
    }

    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Store variables.
        this.context = context;

        storageHandlers = new Hashtable<Long, THLStorage>();

        // Check storage type to turn off multi-storage handlers.
        try
        {
            THLStorage storageHandler = (THLStorage) Class.forName(storage)
                    .newInstance();
            if (storageHandler instanceof DiskTHLStorage)
            {
                useMultiStorageHandler = false;
            }
        }
        catch (Exception e)
        {
            throw new ReplicatorException("Unable to load storage handler: "
                    + storage, e);
        }
    }

    @Override
    public synchronized void prepare(PluginContext context)
            throws ReplicatorException, InterruptedException
    {
        // Initialize first storage handler
        THLStorage storageHandler = getThlStorageHandler();
        if (useMultiStorageHandler)
        {
            storageHandlers.put(Thread.currentThread().getId(), storageHandler);
            this.adminStorageHandler = getThlStorageHandler();
        }
        else
        {
            storageHandlers.put(-1L, storageHandler);
            this.adminStorageHandler = getStorageHandler();
        }

        // Prepare for operation.
        super.prepare(context);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THL#getStorageHandler()
     */
    @Override
    protected THLStorage getStorageHandler() throws ReplicatorException,
            InterruptedException
    {
        if (useMultiStorageHandler)
            return super.getStorageHandler();
        else
            return storageHandlers.get(-1L);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THL#releaseStorageHandler()
     */
    @Override
    public void releaseStorageHandler()
    {
        if (useMultiStorageHandler)
            super.releaseStorageHandler();
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
        props.setInt("logFileSize", logFileSize);
        props.setBoolean("doChecksum", doChecksum);
        props.setString("logFileRetention", logFileRetention);
        // props.setLong(Replicator.MIN_STORED_SEQNO, getMinStoredSeqno(true));
        // props.setLong(Replicator.MAX_STORED_SEQNO, getMaxStoredSeqno(true));
        return props;
    }
}
