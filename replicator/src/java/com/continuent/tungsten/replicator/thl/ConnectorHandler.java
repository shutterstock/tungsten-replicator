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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.thl;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.thl.log.LogConnection;

/**
 * This class defines a ConnectorHandler
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ConnectorHandler implements ReplicatorPlugin, Runnable
{
    private Server           server    = null;
    private PluginContext    context   = null;
    private Thread           thd       = null;
    private SocketChannel    channel   = null;
    private THL              thl       = null;
    private int              resetPeriod;
    private volatile boolean cancelled = false;
    private volatile boolean finished  = false;

    private static Logger    logger    = Logger.getLogger(ConnectorHandler.class);

    // Implements call-back to check log consistency between client and
    // master.
    class LogValidator implements ProtocolHandshakeResponseValidator
    {
        LogValidator()
        {
        }

        /**
         * Ensure that if the client has supplied log position information we
         * validate that the last epoch number and seqno match our log.
         * 
         * @param handshakeResponse Response from client
         * @throws THLException Thrown if logs appear to diverge
         */
        public void validateResponse(ProtocolHandshakeResponse handshakeResponse)
                throws InterruptedException, THLException
        {
            logger.info("New THL client connection from source ID: "
                    + handshakeResponse.getSourceId());

            long clientLastEpochNumber = handshakeResponse.getLastEpochNumber();
            long clientLastSeqno = handshakeResponse.getLastSeqno();

            if (clientLastEpochNumber < 0 || clientLastSeqno < 0)
            {
                logger.info("Client log checking disabled; not checking for diverging histories");
            }
            else
            {
                LogConnection conn = thl.connect(true);
                try
                {
                    // Look for the indicated event.
                    THLEvent event = null;
                    if (conn.seek(clientLastSeqno))
                        event = conn.next(false);

                    // Did we find it?
                    if (event == null)
                    {
                        throw new THLException(
                                "Client requested non-existent transaction: client source ID="
                                        + handshakeResponse.getSourceId()
                                        + " seqno=" + clientLastSeqno
                                        + " client epoch number="
                                        + clientLastEpochNumber);
                    }
                    else if (event.getEpochNumber() != clientLastEpochNumber)
                    {
                        throw new THLException(
                                "Log epoch numbers do not match: client source ID="
                                        + handshakeResponse.getSourceId()
                                        + " seqno=" + clientLastSeqno
                                        + " server epoch number="
                                        + event.getEpochNumber()
                                        + " client epoch number="
                                        + clientLastEpochNumber);
                    }
                    else
                    {
                        logger.info("Log epoch numbers checked and match: client source ID="
                                + handshakeResponse.getSourceId()
                                + " seqno="
                                + clientLastSeqno
                                + " epoch number="
                                + clientLastEpochNumber);
                    }

                }
                finally
                {
                    if (conn != null)
                    {
                        conn.release();
                    }
                }
            }
        }
    }

    /**
     * Creates a new <code>ConnectorHandler</code> object
     */
    public ConnectorHandler()
    {
    }

    /**
     * Returns true if this handler has terminated and may be discarded.
     */
    public boolean isFinished()
    {
        return finished;
    }

    /**
     * Implements the connector handler loop, which runs until we are
     * interrupted.
     */
    public void run()
    {
        LogConnection connection = null;
        Protocol protocol;
        try
        {
            protocol = new Protocol(context, channel, resetPeriod);
        }
        catch (IOException e)
        {
            logger.error("Unable to start connector handler", e);
            return;
        }
        try
        {
            long minSeqno, maxSeqno;
            maxSeqno = thl.getMaxStoredSeqno();
            minSeqno = thl.getMinStoredSeqno();
            LogValidator logValidator = new LogValidator();

            // TUC-2 Added log validator to check log for divergent
            // epoch numbers on last common sequence number.
            protocol.serverHandshake(logValidator, minSeqno, maxSeqno);

            // Name the thread so that developers can see which source ID we
            // are serving.
            Thread.currentThread().setName(
                    "connector-handler-" + protocol.getClientSourceId());

            // Loop until we are cancelled.
            while (!cancelled)
            {
                ProtocolReplEventRequest request;

                // Get the client request.
                request = protocol.waitReplEventRequest();
                long seqno = request.getSeqNo();
                logger.debug("Request " + seqno);
                long prefetchRange = request.getPrefetchRange();
                short fragno = 0;

                // If we don't have a connection to the log, make it now.
                if (connection == null)
                {
                    connection = thl.connect(true);
                    if (!connection.seek(seqno))
                    {
                        String message = "Requested event (#" + seqno
                                + " / " + fragno + ") not found in database";
                        logger.warn(message);
                        sendError(protocol, message);
                        return;
                    }
                }

                long i = 0;
                while (i < prefetchRange)
                {
                    // Get the next event from the log, waiting if necessary.
                    THLEvent event = connection.next();

                    // Peel off and process the underlying replication event. 
                    ReplEvent revent = event.getReplEvent();
                    if (revent instanceof ReplDBMSEvent
                            && ((ReplDBMSEvent) revent).getDBMSEvent() instanceof DBMSEmptyEvent)
                    {
                        logger.debug("Got an empty event");
                        sendEvent(protocol, revent,
                                (seqno + i >= thl.getMaxCommittedSeqno()));
                        i++;
                        fragno = 0;
                    }
                    else
                    {
                        if (revent instanceof ReplDBMSEvent)
                        {
                            ReplDBMSEvent replDBMSEvent = (ReplDBMSEvent) revent;
                            if (replDBMSEvent.getLastFrag())
                            {
                                if (replDBMSEvent instanceof ReplDBMSFilteredEvent)
                                {
                                    ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) replDBMSEvent;
                                    i += 1 + ev.getSeqnoEnd() - ev.getSeqno();
                                }
                                else
                                {
                                    if (logger.isDebugEnabled())
                                        logger.debug("Last fragment of event "
                                                + replDBMSEvent.getSeqno()
                                                + " reached : "
                                                + replDBMSEvent.getFragno());
                                    i++;
                                }
                                fragno = 0;
                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                    logger.debug("Not the last frag for event "
                                            + replDBMSEvent.getSeqno() + "("
                                            + replDBMSEvent.getFragno() + ")");
                                if (replDBMSEvent instanceof ReplDBMSFilteredEvent)
                                {
                                    ReplDBMSFilteredEvent ev = (ReplDBMSFilteredEvent) replDBMSEvent;
                                    fragno = (short) (ev.getFragnoEnd() + 1);
                                }
                                else
                                    fragno++;
                            }
                        }
                        else
                        {
                            logger.debug("Got " + revent.getClass());
                            i++;
                            fragno = 0;
                        }
                        sendEvent(protocol, revent,
                                (seqno + i >= thl.getMaxCommittedSeqno()));
                    }
                }
            }

        }
        catch (InterruptedException e)
        {
            if (cancelled)
                logger.info("Connector handler cancelled");
            else
                logger.error(
                        "Connector handler terminated by unexpected interrupt",
                        e);
        }
        catch (EOFException e)
        {
            // The EOF exception happens on a slave being promoted to master
            if (logger.isDebugEnabled())
                logger.info(
                        "Connector handler terminated by java.io.EOFException",
                        e);
            else
                logger.info("Connector handler terminated by java.io.EOFException");
        }
        catch (IOException e)
        {
            // The IOException occurs normally when a client goes away.
            if (logger.isDebugEnabled())
                logger.debug("Connector handler terminated by i/o exception", e);
            else
                logger.info("Connector handler terminated by i/o exception");
        }
        catch (THLException e)
        {
            logger.error(
                    "Connector handler terminated by THL exception: "
                            + e.getMessage(), e);
        }
        catch (Throwable t)
        {
            logger.error(
                    "Connector handler terminated by unexpected exception", t);
        }
        finally
        {
            // Release log connection.
            if (connection != null)
                connection.release();

            // Close TCP/IP.
            try
            {
                channel.close();
            }
            catch (Exception e)
            {
                logger.warn("Error on closing connection handle", e);
            }

            // Tell the server we are done.
            server.removeClient(this);

            // Make sure we can see that the connection ended.
            logger.info("Terminating THL client connection from source ID: "
                    + protocol.getClientSourceId());
        }
    }

    private void sendEvent(Protocol protocol, ReplEvent event, boolean forceSend)
            throws IOException
    {
        protocol.sendReplEvent(event, forceSend);
    }

    private void sendError(Protocol protocol, String message)
            throws IOException
    {
        protocol.sendError(message);
    }

    /**
     * Start the thread to serve thl changes to requesting slaves.
     */
    public void start()
    {
        thd = new Thread(this, "ConnectorHandler: initializing");
        thd.start();
    }

    /**
     * Stop the thread which is serving changes to requesting slaves.
     * 
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException
    {
        if (finished)
            return;

        cancelled = true;

        // Stop handler thread.
        try
        {
            thd.interrupt();
            // Bound the wait to prevent hangs.
            thd.join(10000);
            thd = null;
        }
        catch (InterruptedException e)
        {
            // This is a possible JDK bug or at least inscrutable behavior.
            // First call to Thread.join() when deallocating threads seems
            // to trigger an immediate interrupt.
            logger.warn("Connector handler stop operation was interrupted");
            if (thd != null)
                thd.join();
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
        this.context = context;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        resetPeriod = thl.getResetPeriod();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
    }

    /**
     * Sets the server value.
     * 
     * @param server The server to set.
     */
    public void setServer(Server server)
    {
        this.server = server;
    }

    /**
     * Sets the channel value.
     * 
     * @param channel The channel to set.
     */
    public void setChannel(SocketChannel channel)
    {
        this.channel = channel;
    }

    /**
     * Sets the thl value.
     * 
     * @param thl The thl to set.
     */
    public void setThl(THL thl)
    {
        this.thl = thl;
    }

}
