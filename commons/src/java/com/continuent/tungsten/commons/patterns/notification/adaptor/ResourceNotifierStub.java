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
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.patterns.notification.adaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotifier;

/**
 * This is a stub that demonstrates the components of a ResourceNotifier in
 * action.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class ResourceNotifierStub implements ResourceNotifier
{

    private static Logger                            logger    = Logger
                                                                       .getLogger(ResourceNotifierStub.class);

    private volatile Boolean                         shutdown  = new Boolean(
                                                                       false);
    static private Thread                            monThread = null;
    private Collection<ResourceNotificationListener> listeners = new ArrayList<ResourceNotificationListener>();

    /**
     * @param argv
     */
    public static void main(String argv[])
    {

        ResourceNotifierStub adaptor = null;

        try
        {
            adaptor = new ResourceNotifierStub();
            monThread = new Thread(adaptor, adaptor.getClass().getSimpleName());
            monThread.setDaemon(true);
            monThread.start();
            // Wait for the monitor thread to exit....
            monThread.wait();
        }
        catch (InterruptedException i)
        {
            logger.info("Exiting after interruption....");
            System.exit(0);
        }

    }

    /**
     * @param listener
     */
    public void addListener(ResourceNotificationListener listener)
    {
        listeners.add(listener);
    }

    /**
     * @param notification
     */
    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    /**
     ** Simulates the reception of a notification every 20 seconds.
     */
    public void run()
    {

        logger.debug("ResourceNotifierStub MONITOR: STARTED");

        while (!shutdown)
        {
            try
            {
                Thread.sleep(20000);

                // Do a notification here if you want to....
            }
            catch (Exception e)
            {
                System.err.println(e);
            }
        }
    }

    /**
     * 
     */
    public void shutdown()
    {
        synchronized (shutdown)
        {
            shutdown = false;
            shutdown.notify();
        }
    }

    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        return new HashMap<String, NotificationGroupMember>();
    }

    public void prepare() throws Exception
    {
        // TODO Auto-generated method stub
        
    }

}
