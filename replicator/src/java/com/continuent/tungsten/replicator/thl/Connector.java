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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;

/**
 * This class defines a Connector
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class Connector implements ReplicatorPlugin
{
    private static Logger   logger        = Logger.getLogger(Connector.class);

    protected PluginContext pluginContext = null;
    protected String        host          = null;
    protected int           port          = 2112;
    private SocketChannel   channel       = null;
    private long            minSeqNo      = -1;
    private long            maxSeqNo      = -1;
    private Protocol        protocol      = null;

    protected int           resetPeriod;
    protected long          lastSeqno;
    protected long          lastEpochNumber;

    private String          remoteURI     = null;

    /**
     * Creates a new <code>Connector</code> object
     */
    public Connector()
    {
    }

    /**
     * Creates a new <code>Connector</code> object Creates a new
     * <code>Connector</code> object
     * 
     * @param remoteURI URI string pointing to remote host
     * @param resetPeriod output stream resetting period
     * @throws ReplicatorException
     */
    public Connector(PluginContext context, String remoteURI, int resetPeriod,
            long lastSeqno, long lastEpochNumber) throws ReplicatorException
    {
        this.pluginContext = context;
        this.lastSeqno = lastSeqno;
        this.lastEpochNumber = lastEpochNumber;
        try
        {
            URI uri = new URI(remoteURI);
            this.host = uri.getHost();
            if ((this.port = uri.getPort()) == -1)
                this.port = 2112;
            this.resetPeriod = resetPeriod;
        }
        catch (URISyntaxException e)
        {
            throw new THLException(e.getMessage());
        }
    }

    /**
     * TODO: connect definition.
     * 
     * @throws ReplicatorException
     * @throws IOException
     */
    public void connect() throws ReplicatorException, IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Connecting to " + host + ":" + port);
        try
        {
            channel = SocketChannel.open(new InetSocketAddress(host, port));
        }
        catch (UnresolvedAddressException e)
        {
            throw new THLException(
                    "THL connection failure; cannot resolve address: host="
                            + host + " port=" + port);
        }
        catch (UnsupportedAddressTypeException e)
        {
            throw new THLException(
                    "THL connection failure; address is invalid: host=" + host
                            + " port=" + port);
        }

        // Disable Nagle's algorithm
        channel.socket().setTcpNoDelay(true);
        // Enable TCP keepalive
        channel.socket().setKeepAlive(true);
        protocol = new Protocol(pluginContext, channel, resetPeriod);
        SeqNoRange seqNoRange = protocol.clientHandshake(lastEpochNumber,
                lastSeqno);
        minSeqNo = seqNoRange.getMinSeqNo();
        maxSeqNo = seqNoRange.getMaxSeqNo();
    }

    /**
     * Close channel. This is synchronized to prevent accidental double calls.
     */
    public synchronized void close()
    {
        if (channel != null)
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                logger.warn(e.getMessage());
            }
            finally
            {
                channel = null;
            }
        }
    }

    /**
     * Fetch an event from server.
     * 
     * @param seqNo
     * @return ReplEvent
     * @throws ReplicatorException
     * @throws IOException
     */
    public ReplEvent requestEvent(long seqNo) throws ReplicatorException,
            IOException
    {
        ReplEvent retval;
        if (logger.isDebugEnabled())
            logger.debug("Requesting event " + seqNo);
        retval = protocol.requestReplEvent(seqNo);
        if (logger.isDebugEnabled() && retval instanceof ReplDBMSEvent)
        {
            ReplDBMSEvent ev = (ReplDBMSEvent) retval;
            logger.debug("Received event " + ev.getSeqno() + "/"
                    + ev.getFragno());
        }
        return retval;
    }

    /**
     * TODO: getMinSeqNo definition.
     * 
     * @return min seqno
     */
    public long getMinSeqNo()
    {
        return minSeqNo;
    }

    /**
     * TODO: getMaxSeqNo definition.
     * 
     * @return max seqno
     */
    public long getMaxSeqNo()
    {
        return maxSeqNo;
    }

    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        this.pluginContext = context;
        URI uri = null;
        try
        {
            uri = new URI(remoteURI);
        }
        catch (URISyntaxException e)
        {
            throw new THLException(e);
        }
        this.host = uri.getHost();
        if ((this.port = uri.getPort()) == -1)
            this.port = 2112;
    }

    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * Sets the resetPeriod value.
     * 
     * @param resetPeriod The resetPeriod to set.
     */
    public void setResetPeriod(int resetPeriod)
    {
        this.resetPeriod = resetPeriod;
    }

    /**
     * Sets the lastSeqno value.
     * 
     * @param lastSeqno The lastSeqno to set.
     */
    public void setLastSeqno(long lastSeqno)
    {
        this.lastSeqno = lastSeqno;
    }

    /**
     * Sets the lastEpochNumber value.
     * 
     * @param lastEpochNumber The lastEpochNumber to set.
     */
    public void setLastEpochNumber(long lastEpochNumber)
    {
        this.lastEpochNumber = lastEpochNumber;
    }

    public void setURI(String connectUri)
    {
        this.remoteURI = connectUri;
    }
}
