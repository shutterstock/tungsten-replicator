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
package com.continuent.tungsten.commons.patterns.notification;

import java.util.*;
import java.util.concurrent.*;

import org.apache.log4j.*;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;

public class QueueingNotificationListener
        implements
            ResourceNotificationListener,
            Runnable
{
    private Thread                             runner        = null;
    private static Logger                      logger        = Logger
                                                                     .getLogger(QueueingNotificationListener.class);

    private BlockingQueue<Map<String, Object>> notifications = new SynchronousQueue<Map<String, Object>>();
    private String                             type          = null;

   
    public void init(String type)
    {
        // STUB
    }
    
    public void notify(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        notify(notification);
    }

   

    /**
     * Put a notification on the appropriate queue
     */
    public void putNotification(Map<String, Object> notification)
            throws ResourceNotificationException
    {
        synchronized (notifications)
        {

            try
            {
                notifications.put(notification);
            }
            catch (InterruptedException i)
            {
                throw new ResourceNotificationException(
                        "Interrupted while trying to put notification of type="
                                + type);
            }
        }
    }

    /**
     * Gets the next notification in the queue
     */
    public Map<String, Object> getNotification()
            throws ResourceNotificationException
    {
        Map<String, Object> notification = null;

        synchronized (notifications)
        {

            try
            {
                notification = notifications.take();
            }
            catch (InterruptedException i)
            {
                throw new ResourceNotificationException(
                        "Interrupted while waiting for notification of type="
                                + type);
            }

            return notification;
        }
    }

    private void processNotifications() throws ResourceNotificationException
    {

        Map<String, Object> notification = null;

        /*
         * Because notifications are posted on a BlockingQueue, the
         * getNotification() method call will block, waiting for new
         * notifications and will then process them synchronously.
         */
        while ((notification = getNotification()) != null)
        {

            logger.info("PROCESSING NOTIFICATION=" + notification);

        }
    }

    public void run()
    {
        try
        {
            processNotifications();
        }
        catch (ResourceNotificationException r)
        {
            logger.error("Exception while processing notifications:" + r);
            return;
        }
    }

    public void start()
    {
        runner = new Thread(this);
        runner.start();
        
        
    }

}
