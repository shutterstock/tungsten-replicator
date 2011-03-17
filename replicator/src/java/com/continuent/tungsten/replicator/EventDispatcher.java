/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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

package com.continuent.tungsten.replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * 
 * This class defines an event dispatcher.  The dispatcher is a separate
 * thread that dispatches events to listeners. 
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class EventDispatcher implements Runnable
{
    private static Logger logger = Logger.getLogger(EventDispatcher.class);
    private Thread                      th            = null;
    private BlockingQueue<EventRequest> notifications = new LinkedBlockingQueue<EventRequest>();
    private boolean                     running       = false;
    private List<EventListener>         listeners     = new ArrayList<EventListener>();

    public void addListener(EventListener listener)
    {
        listeners.add(listener);
    }

    /**
     * Runs the thread, which continues until interrupted or running is 
     * set to false. 
     * 
     * {@inheritDoc}
     * @see java.lang.Runnable#run()
     */
    public void run()
    {
        try
        {
            do
            {
                boolean succeeded = true;
                ReplicatorException exception = null;
                EventRequest request = notifications.take();
                try
                {
                    for (EventListener listener : listeners)
                    {
                        listener.onEvent(request.getEvent());
                    }
                }
                catch (ReplicatorException e)
                {
                    // Record error information. 
                    logger.debug("Exception in event loop", e);
                    succeeded = false;
                    exception = e;
                }
                
                // Return a response if desired. 
                if (request.getResponseQueue() != null)
                {
                    EventStatus status = new EventStatus(succeeded, exception);
                    request.getResponseQueue().put(status);
                }
            }
            while (running);
        }
        catch (InterruptedException e)
        {
            logger.debug("Dispatcher loop terminated by InterruptedException");
        }
        running = false;
    }
    
    /**
     * Puts a single event in the queue for asynchronous processing. This model
     * supports one-way processing where the client does not worry about when
     * the event is handled or the result of such processing.
     */
    public void handleEvent(Event event) throws InterruptedException
    {
        notifications.put(new EventRequest(event, null));
    }
    
    /**
     * Puts an event in the queue for synchronous processing. This method
     * returns when the event is processed and puts a response back in the
     * response queue. The thread is blocked until the response returns.
     */
    public void handleEventSynchronous(Event event)
            throws InterruptedException, ReplicatorException
    {
        BlockingQueue<EventStatus> responseQueue = new LinkedBlockingQueue<EventStatus>();
        notifications.put(new EventRequest(event, responseQueue));
        EventStatus status = responseQueue.take();
        ReplicatorException replicatorException = status.getReplicatorException();
        if (replicatorException != null)
            throw replicatorException;
    }
    
    /**
     * Start the event dispatcher, which spawns a separate thread. 
     */
    public synchronized void start() throws Exception
    {
        logger.info("Starting event dispatcher");
        if (running == true)
            throw new Exception("NotificationListener already running");
        th = new Thread(this, "Event dispatcher thread");
        running = true;
        th.start();
    }
    
    /**
     * Cancel the event dispatcher and wait for the thread to complete. 
     */
    public synchronized void stop() throws InterruptedException
    {
        if (running == false)
            return;
        logger.info("Stopping event dispatcher");
        running=false;
        th.interrupt();
        th.join();
    }
}
