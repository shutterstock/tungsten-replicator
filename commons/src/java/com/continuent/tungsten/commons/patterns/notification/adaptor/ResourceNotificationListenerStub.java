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
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.commons.patterns.notification.adaptor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;

public class ResourceNotificationListenerStub
        implements
            ResourceNotificationListener,
            Runnable
{

    private Logger                                     logger        = Logger
                                                                             .getLogger(ResourceNotificationListenerStub.class);
    private BlockingQueue<ClusterResourceNotification> notifications = new LinkedBlockingQueue<ClusterResourceNotification>();
    Thread                                             monitorThread = null;
    private String                                     type;

    public void init(String type)
    {
        this.type = type;
    }

    public void start()
    {
        monitorThread = new Thread(this, this.getClass().getSimpleName());
        monitorThread.setDaemon(true);
        monitorThread.start();

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener#notify(ClusterResourceNotification)
     */
    public void notify(ClusterResourceNotification notification)
    {
        logger.debug("GOT NOTIFICATION TYPE=" + type + ":" + notification);

        try
        {
            notifications.put(notification);
        }
        catch (InterruptedException i)
        {
            // IGNORED
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        ClusterResourceNotification notification = null;

        /*
         * Because notifications are posted on a BlockingQueue, the
         * getNotification() method call will block, waiting for new
         * notifications and will then process them synchronously.
         */
        try
        {
            while ((notification = notifications.take()) != null)
            {

                logger.debug("PROCESSING NOTIFICATION FOR TYPE=" + type + ":"
                        + notification);

            }
        }
        catch (InterruptedException i)
        {
            // IGNORED
        }

    }

    public void setData(Object data)
    {
        // TODO Auto-generated method stub

    }

}