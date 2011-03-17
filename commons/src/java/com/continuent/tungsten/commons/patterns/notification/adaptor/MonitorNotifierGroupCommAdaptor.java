/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.patterns.notification.adaptor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.continuent.hedera.adapters.MessageListener;
import org.continuent.hedera.adapters.PullPushAdapter;
import org.continuent.hedera.channel.AbstractReliableGroupChannel;
import org.continuent.hedera.common.GroupIdentifier;
import org.continuent.hedera.common.Member;
import org.continuent.hedera.factory.AbstractGroupCommunicationFactory;
import org.continuent.hedera.gms.AbstractGroupMembershipService;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotifier;

/**
 * This class represents a means to receive monitoring information about
 * datasources from group communications. This works with a variety of GC
 * implementations thanks to using Hedera wrappers.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class MonitorNotifierGroupCommAdaptor
        implements
            MessageListener,
            ResourceNotifier

{
    private static Logger                            logger      = Logger
                                                                         .getLogger(MonitorNotifierGroupCommAdaptor.class);
    private Collection<ResourceNotificationListener> listeners   = new ArrayList<ResourceNotificationListener>();

    /** Name of the hedera.properties file, which must be on the class path. */
    private String                                   properties  = "/hedera.monitoring.properties";

    /** Name of the group used for exchanging monitoring notifications. */
    private String                                   channelName = "monitoring";

    // Group communications information.
    private GroupIdentifier                          gid         = null;
    private AbstractReliableGroupChannel             channel;
    private AbstractGroupMembershipService           gms;
    private PullPushAdapter                          adapter;

    /**
     * Create a new adapter.
     */
    public MonitorNotifierGroupCommAdaptor()
            throws ResourceNotificationException
    {
        configureGroupCommunications();
    }

    /**
     * Add a listener that will be informed in the event of a resource
     * notification.
     * 
     * @param listener
     */
    public void addListener(ResourceNotificationListener listener)
    {
        if (listener == null)
        {
            logger.error("Attempting to add null listener");
        }
        listeners.add(listener);
    }

    /**
     * Deliver notification to listeners.
     * 
     * @param notification
     */
    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        if (notification == null)
            return;

        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    /**
     * Run start the listener thread. It turns out that Group communications
     * don't really need a separate thread since the group comm adapters have
     * their own threads. So we start group comm instead.
     */
    public void run()
    {
        logger.debug("MONITOR: STARTED");
        startGroupCommunications();
    }

    public void configureGroupCommunications()
            throws ResourceNotificationException
    {
        // Load hedera properties.
        if (logger.isDebugEnabled())
        {
            logger.debug("Initializing group communications");
            logger.debug("Hedera properties file: " + properties);
            logger.debug("Channel name: " + channelName);
        }

        Properties p = new Properties();

        p = new Properties();

        String clusterHome = System.getProperty("cluster.home");

        if (clusterHome == null)
        {
            throw new ResourceNotificationException(
                    String
                            .format(
                                    "Unable to find hedera properties file %s because the cluster.home system property is not set",
                                    properties));

        }
        String propertiesPath = String.format("%s/conf/%s", clusterHome,
                properties);

        try
        {
            InputStream is = new FileInputStream(propertiesPath);
            p.load(is);
            is.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Unable to load hedera.properties", e);
            }
            throw new ResourceNotificationException(String.format(
                    "Unable to load file '%s', reason=%s", propertiesPath, e
                            .getMessage()));
        }
        logger.debug("Able to load properties");

        channelName = p.getProperty("hedera.channel.name", channelName);

        if (logger.isDebugEnabled())
        {
            logger.debug("Channel name: " + channelName);
        }

        // Start the factory.
        AbstractGroupCommunicationFactory factory = null;
        try
        {
            factory = AbstractGroupCommunicationFactory.getFactory(p);
        }
        catch (Exception e)
        {
            throw new ResourceNotificationException(
                    "Unable to open group communications factory", e);
        }
        logger.debug("Able to start communications factory: "
                + factory.getClass().getSimpleName());

        // Open the communication channel and group membership service.
        gid = new GroupIdentifier(channelName);

        try
        {
            Object[] ret = factory.createChannelAndGroupMembershipService(p,
                    gid);
            channel = (AbstractReliableGroupChannel) ret[0];
            gms = (AbstractGroupMembershipService) ret[1];
        }
        catch (Exception e)
        {
            throw new ResourceNotificationException(
                    "Unable to open channel and membership service", e);
        }
        logger.debug("Able to start channel: "
                + channel.getClass().getSimpleName());
        logger.debug("Able to start group membership service: "
                + gms.getClass().getSimpleName());

        // Join the GC channel.
        try
        {
            channel.join();
        }
        catch (Exception e)
        {
            throw new ResourceNotificationException(
                    "Unable to join group communications channel", e);
        }
        logger.debug("Joined channel");
    }

    /**
     * Start group communications by launching the message adapter thread.
     */
    public void startGroupCommunications()
    {
        // Start message adapter.

        if (logger.isDebugEnabled())
        {
            logger.debug("Starting GC message adapter");
        }
        adapter = new PullPushAdapter(channel, this);
        adapter.start();

        if (logger.isDebugEnabled())
        {
            logger.info("Group communication configuration is complete");
        }
    }

    /**
     * Shut down group communications.
     */
    protected void stopGroupCommunications()
    {
        logger.info("Shutting down connection to group communications");
        try
        {
            adapter.stop();
            channel.quit();
        }
        catch (Exception e)
        {
            logger.warn("Unable to close group communications cleanly", e);
        }
    }
    
    /**
     * Shut down by logging out of the channel.
     */
    public void shutdown()
    {
        stopGroupCommunications();
    }

    /**
     * Receive a resource notification. This should be a HashMap.
     */
    public void receive(Serializable object)
    {
        try
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Received resource notification: " + object);
            }

            ClusterResourceNotification notification = (ClusterResourceNotification) object;
            this.notifyListeners(notification);

        }
        catch (Throwable e)
        {
            logger.error(
                    "Unable to receive and deliver resource notification: "
                            + object, e);
        }
    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        LinkedHashMap<String, NotificationGroupMember> members = new LinkedHashMap<String, NotificationGroupMember>();

        for (Member channelMember : channel.getGroup().getMembers())
        {
            SocketAddress sock = channelMember.getAddress().getAddress();
            String name = channelMember.getAddress().toString();
            String host;
            int port;

            if (sock instanceof InetSocketAddress)
            {
                host = ((InetSocketAddress) sock).getHostName();
                port = ((InetSocketAddress) sock).getPort();
            }
            else
            {
                host = channelMember.getAddress().toString();
                port = channelMember.getAddress().getPort();
            }

            NotificationGroupMember newMember = new NotificationGroupMember(
                    name, host, port);

            String generalName = NotificationGroupMember
                    .getMemberAddress(newMember.getName()); // for debugging
            members.put(generalName, newMember);
        }

        return members;
    }

    public void prepare() throws Exception
    {
        // TODO Auto-generated method stub
        
    }

}
