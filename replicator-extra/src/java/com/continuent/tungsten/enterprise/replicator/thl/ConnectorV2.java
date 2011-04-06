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

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.thl.Connector;
import com.continuent.tungsten.replicator.thl.Protocol;
import com.continuent.tungsten.replicator.thl.SeqNoRange;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class defines a Connector
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ConnectorV2 extends Connector
{
    private static Logger logger        = Logger.getLogger(ConnectorV2.class);

//    private PluginContext pluginContext = null;
//    private String        host          = null;
//    private int           port          = 2112;
    private SocketChannel channel       = null;
    private long          minSeqNo      = -1;
    private long          maxSeqNo      = -1;
    private Protocol      protocol      = null;
//    private int           resetPeriod;
//    private long          lastSeqno;
//    private long          lastEpochNumber;

//    private String        remoteURI     = null;

    /**
     * Creates a new <code>Connector</code> object
     */
    public ConnectorV2()
    {
    }


    /**
     * TODO: connect definition.
     * 
     * @throws THLException
     * @throws IOException
     */
    public void connect() throws THLException, IOException
    {
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
        protocol = new ProtocolV2(pluginContext, channel, resetPeriod, null);
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
     * @throws THLException
     * @throws IOException
     */
    public ReplEvent requestEvent(long seqNo) throws THLException, IOException
    {
        ReplEvent retval;
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
}
