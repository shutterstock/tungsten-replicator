/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Robert Hodges.
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.commons.patterns.notification.adaptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.continuent.hedera.adapters.MessageListener;
import org.continuent.hedera.adapters.PullPushAdapter;
import org.continuent.hedera.channel.AbstractReliableGroupChannel;
import org.continuent.hedera.channel.NotConnectedException;
import org.continuent.hedera.common.Group;
import org.continuent.hedera.common.GroupIdentifier;
import org.continuent.hedera.common.IpAddress;
import org.continuent.hedera.common.Member;
import org.continuent.hedera.factory.AbstractGroupCommunicationFactory;
import org.continuent.hedera.gms.AbstractGroupMembershipService;
import org.continuent.hedera.gms.GroupMembershipListener;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotifier;

/**
 * This class defines a GroupCommunicationsNotifier
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class GroupCommunicationsNotifier
        implements
            ResourceNotifier,
            MessageListener,
            GroupMembershipListener
{

    private static final Logger            logger      = Logger.getLogger(GroupCommunicationsNotifier.class);

    /** Name of the hedera.properties file, which must be on the class path. */
    private String                         properties  = "/hedera.properties";

    /** Name of the group used for exchanging monitoring notifications. */
    private String                         channelName = "monitoring";

    private int                            joinDelay   = 20;

    // Group communications information.
    private GroupIdentifier                gid         = null;
    private AbstractReliableGroupChannel   channel;
    private AbstractGroupMembershipService gms;
    private PullPushAdapter                adapter;

    /** Creates a new instance. */
    public GroupCommunicationsNotifier()
    {
    }

    public void setProperties(String properties)
    {
        this.properties = properties;
    }

    public void setChannelName(String channelName)
    {
        this.channelName = channelName;
    }

    /**
     * Connects the notifier to group communications using generic Hedera
     * interfaces. {@inheritDoc}
     *
     * @see com.continuent.tungsten.commons.patterns.notification.ResourceNotifier#prepare()
     */
    public void prepare() throws Exception
    {
        // Load hedera properties.
        logger.info("Initializing group communications");
        logger.info("Hedera properties file: " + properties);
        logger.info("Channel name: " + channelName);

        Properties p = new Properties();
        try
        {
            p = new Properties();
            URL propertiesUrl = this.getClass().getResource(properties);
            if (propertiesUrl == null)
                throw new NotificationAdaptorException(
                        "Unable to find hedera properties in class path: "
                                + properties);

            InputStream is = propertiesUrl.openStream();
            p.load(is);
            is.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Unable to load hedera.properties", e);
            }
            throw new NotificationAdaptorException(
                    "Unable to load hedera.properties: " + properties);
        }
        logger.debug("Able to load properties");

        // Start the factory.
        AbstractGroupCommunicationFactory factory = null;
        try
        {
            factory = AbstractGroupCommunicationFactory.getFactory(p);
        }
        catch (Exception e)
        {
            throw new NotificationAdaptorException(
                    "Unable to open group communications factory", e);
        }
        logger.debug("Able to start communications factory: "
                + factory.getClass().getName());

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
            throw new NotificationAdaptorException(
                    "Unable to open channel and membership service", e);
        }
        logger.debug("Able to start channel: " + channel.getClass().getName());
        logger.debug("Able to start group membership service: "
                + gms.getClass().getName());

        // Join the GC channel.
        try
        {
            channel.join();
        }
        catch (Exception e)
        {
            throw new NotificationAdaptorException(
                    "Unable to join group communications channel", e);
        }
        logger.debug("Joined channel");

        // Start message adapter.
        adapter = new PullPushAdapter(channel, this);
        adapter.start();
        logger.debug("Started message adapter");

        gms.registerGroupMembershipListener(this);
        logger.info("Registered for group membership changes");
        logger.info("Group communication configuration is complete");
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.commons.patterns.notification.ResourceNotifier#notifyListeners(com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification)
     */
    public synchronized void notifyListeners(
            ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        // Pop it into group communications.
        if (logger.isDebugEnabled())
        {
            logger.debug("Sending message: " + notification);
        }
        try
        {
            adapter.send((Serializable) notification);
        }
        catch (NotConnectedException e)
        {
            // We should connect here.
            throw new ResourceNotificationException(
                    "Could not send message: channel is not connected", e);
        }
    }

    /**
     * TODO: release definition.
     */
    public void release()
    {
        logger.info("Shutting down connection to group communications");
        try
        {
            channel.quit();
        }
        catch (Exception e)
        {
            logger.warn("Unable to close group communications cleanly", e);
        }
    }

    /**
     * Receives a message from group communications. We only print such messages
     * if we are in debug mode. {@inheritDoc}
     *
     * @see org.continuent.hedera.adapters.MessageListener#receive(java.io.Serializable)
     */
    public void receive(Serializable message)
    {
        if (logger.isDebugEnabled())
            logger.debug("Received message: " + message);
    }

    public int getJoinDelay()
    {
        return joinDelay;
    }

    public void setJoinDelay(int joinDelay)
    {
        this.joinDelay = joinDelay;
    }

    public void failedMember(Member arg0, GroupIdentifier arg1, Member arg2)
    {
        logger.info(String.format("NOTIFIER: FAILED(%s)", arg0));
    }

    public void groupComposition(Group arg0, IpAddress arg1, int arg2)
    {
        // TODO Auto-generated method stub

    }

    public void joinMember(Member arg0, GroupIdentifier arg1)
    {
        logger.info(String.format("NOTIFIER: JOINED(%s)", arg0));
    }

    public void mergedMembers(GroupIdentifier arg0, Member arg1,
            List<Member> arg2, List<Member> arg3, List<Member> arg4)
    {
        // TODO Auto-generated method stub

    }

    @SuppressWarnings("unchecked")
    public void networkPartition(GroupIdentifier arg0, List arg1)
    {
        // TODO Auto-generated method stub

    }

    public void quitMember(Member arg0, GroupIdentifier arg1)
    {
        logger.info(String.format("NOTIFIER: QUIT(%s)", arg0));

    }

    public void suspectMember(Member arg0, GroupIdentifier arg1, Member arg2)
    {
        // TODO Auto-generated method stub

    }

    public void addListener(ResourceNotificationListener listener)
    {
        // TODO Auto-generated method stub

    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void run()
    {
        // TODO Auto-generated method stub

    }
}
