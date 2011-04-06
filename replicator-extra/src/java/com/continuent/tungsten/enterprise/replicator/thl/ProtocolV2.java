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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.Protocol;
import com.continuent.tungsten.replicator.thl.ProtocolMessage;
import com.continuent.tungsten.replicator.thl.ProtocolReplEventRequest;
import com.continuent.tungsten.replicator.thl.THL;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;

/**
 * This class defines a Protocol
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ProtocolV2 extends Protocol
{
    private static Logger        logger                   = Logger
                                                                  .getLogger(ProtocolV2.class);
    // prefetchRange is a number of sequence number that are fetched
    // automatically (no need to send a message to the master for each sequence
    // number). Warning : a sequence number can be found several times in the
    // history table when the transaction was fragmented.
    private long                 prefetchRange            = Long.MAX_VALUE;
    private long                 prefetchIndex            = 0;
    private boolean              allPreviousFragmentsDone = true;

    private BufferedInputStream  bIS                      = null;
    private BufferedOutputStream bOS                      = null;
    private THL                  thl;

    /**
     * Creates a new <code>Protocol</code> object
     * 
     * @param channel
     * @throws IOException
     */
    public ProtocolV2(PluginContext context, SocketChannel channel)
            throws IOException
    {
        this.pluginContext = context;
        this.channel = channel;

        bOS = new BufferedOutputStream(this.channel.socket().getOutputStream());

        oos = new ObjectOutputStream(bOS);
        // oos.flush();
        bOS.flush();

        resetPeriod = 1;
    }

    public ProtocolV2(PluginContext context, SocketChannel channel,
            int resetPeriod, THL thl) throws IOException
    {
        this(context, channel);
        this.resetPeriod = resetPeriod;
        this.thl = thl;
    }

    /**
     * TODO: writeMessage definition.
     * 
     * @param msg
     * @throws IOException
     * @throws RemoteProtocolException
     */
    private void writeMessage(byte[] data) throws IOException
    {
        int size = data.length;
        logger.debug("Writing data size = " + size);
        oos.writeInt(size);
        oos.flush();

        bOS.write(data);
        bOS.flush();

        logger.debug("Data was successfully written ");
    }

    // private String hexdump(byte[] buffer, int offset)
    // {
    // StringBuffer dump = new StringBuffer();
    // if ((buffer.length - offset) > 0)
    // {
    // dump.append(String.format("%02x", buffer[offset]));
    // for (int i = offset + 1; i < buffer.length; i++)
    // {
    // dump.append("_");
    // dump.append(String.format("%02x", buffer[i]));
    // }
    // }
    // return dump.toString();
    // }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.Protocol#sendReplEvent(com.continuent.tungsten.replicator.event.ReplEvent)
     */
    //@Override
    public void sendReplEvent(ReplEvent event) throws IOException
    {
        throw new IOException(
                "This version of the protocol does not support this operation");
    }

    public void sendDataAsByte(byte[] data) throws IOException
    {
        writeMessage(data);
    }

    public ReplEvent requestReplEvent(long seqNo) throws THLException,
            IOException
    {
        byte[] data = requestData(seqNo);

        ObjectInputStream oIS = new ObjectInputStream(new ByteArrayInputStream(
                data));
        try
        {
            oIS.readInt();
            THLEvent thlEvent = ((DiskTHLStorage) thl.getThlStorageHandler()).find(seqNo);
            return thlEvent.getReplEvent();
        }
        catch (ReplicatorException e)
        {
            throw new THLException("Problem when deserializing event", e);
        }
        catch (InterruptedException e)
        {
            throw new THLException("Problem when deserializing event", e);
        }
        finally
        {
            oIS.close();
        }
    }

    public byte[] requestData(long seqNo) throws THLException, IOException
    {
        if (prefetchIndex == 0 && allPreviousFragmentsDone)
        {
            writeMessage(new ProtocolReplEventRequest(seqNo, prefetchRange));
        }
        prefetchIndex = (prefetchIndex + 1) % prefetchRange;
        return readDataAsByte();
        // ProtocolMessage msg = readMessage();
        // if (msg instanceof ProtocolReplEvent == false)
        // throw new THLException("Protocol error");
        // ReplEvent ret = ((ProtocolReplEvent) msg).getReplEvent();
        // if (ret instanceof ReplDBMSEvent)
        // {
        // if (((ReplDBMSEvent) ret).getLastFrag())
        //
        // {
        // allPreviousFragmentsDone = true;
        // if (ret instanceof ReplDBMSFilteredEvent)
        // {
        // ReplDBMSFilteredEvent event = (ReplDBMSFilteredEvent) ret;
        //
        // if ((1 + prefetchIndex + event.getSeqnoEnd() - event
        // .getSeqno()) > prefetchRange)
        // prefetchIndex = 0;
        // else
        // prefetchIndex = (1 + prefetchIndex
        // + event.getSeqnoEnd() - event.getSeqno())
        // % prefetchRange;
        // }
        // else
        // prefetchIndex = (prefetchIndex + 1) % prefetchRange;
        // }
        // else
        // {
        // allPreviousFragmentsDone = false;
        // }
        // }
        // else
        // {
        // allPreviousFragmentsDone = true;
        // prefetchIndex = (prefetchIndex + 1) % prefetchRange;
        // }
        //
        // return ret;
    }

    private byte[] readDataAsByte() throws IOException
    {
        byte[] data = null;

        long metricID = 0L;
        if (bIS == null)
        {
            bIS = new BufferedInputStream(this.channel.socket()
                    .getInputStream());
            ois = new ObjectInputStream(bIS);
        }
        if (pluginContext.getMonitor().getDetailEnabled())
            metricID = pluginContext.getMonitor().startCPUEvent(
                    ReplicatorMonitor.CPU_MSG_DESERIAL);
        int size = ois.readInt();
        // logger.warn("Reading data = " + size);
        data = new byte[size];
        int readBytes = 0;

        while (readBytes < size)
        {
            readBytes += bIS.read(data, readBytes, size - readBytes);
            // logger.info("Read " + readBytes + " out of " + size);
        }

        logger.debug("Expected " + size + " bytes - Read " + readBytes
                + " bytes.");

        if (pluginContext.getMonitor().getDetailEnabled())
            pluginContext.getMonitor().stopCPUEvent(
                    ReplicatorMonitor.CPU_MSG_DESERIAL, metricID);

        return data;
    }

    protected ProtocolMessage readMessage() throws IOException, THLException
    {
        if (ois == null)
        {
            bIS = new BufferedInputStream(this.channel.socket()
                    .getInputStream());
            ois = new ObjectInputStream(bIS);
        }
        return super.readMessage();
    }

}
