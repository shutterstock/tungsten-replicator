/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009-2010 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements a "watch" operation that waits for a predicate to be fulfilled on
 * a particular event in an event processing queue.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges/a>
 */
public class Watch<E> implements Future<E>
{
    private final WatchPredicate<E>          predicate;
    private final WatchAction<E>             action;
    private final BlockingQueue<EventHolder> responseQueue = new LinkedBlockingQueue<EventHolder>();
    private final boolean[]                  matched;
    private boolean                          cancelled     = false;
    private boolean                          done          = false;

    // Defines a wrapper class to hold events in the queue. The wrapper
    // allows us to insert a null event for cancellation.
    class EventHolder
    {
        private final E event;

        EventHolder(E event)
        {
            this.event = event;
        }

        E getEvent()
        {
            return event;
        }
    }

    /**
     * Create watch with predicate and task count.
     */
    public Watch(WatchPredicate<E> predicate, int taskCount)
    {
        this(predicate, taskCount, null);
    }

    /**
     * Create watch with all components.
     * 
     * @param predicate Predicate to match
     * @param action Action to execute
     * @param taskCount Number of tasks that must report for a match
     */
    public Watch(WatchPredicate<E> predicate, int taskCount,
            WatchAction<E> action)
    {
        this.predicate = predicate;
        this.action = action;
        this.matched = new boolean[taskCount];
    }

    /**
     * Returns the watch predicate.
     */
    public WatchPredicate<E> getPredicate()
    {
        return predicate;
    }

    /**
     * Returns the action or null if no action is defined.
     */
    public WatchAction<E> getAction()
    {
        return action;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#cancel(boolean)
     */
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        try
        {
            responseQueue.put(new EventHolder(null));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        cancelled = true;
        done = true;
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#get()
     */
    public E get() throws InterruptedException, ExecutionException
    {
        EventHolder holder = responseQueue.take();
        if (cancelled)
            throw new CancellationException();
        else
        {
            done = true;
            return holder.getEvent();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
     */
    public E get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        EventHolder holder = responseQueue.poll(timeout, unit);
        if (cancelled)
            throw new CancellationException("This watch was cancelled");
        else if (holder == null)
            throw new TimeoutException();
        else
        {
            done = true;
            return holder.getEvent();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#isCancelled()
     */
    public boolean isCancelled()
    {
        return cancelled;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.util.concurrent.Future#isDone()
     */
    public boolean isDone()
    {
        return done;
    }

    /**
     * Offer an event to this watch instance. If it accepts the event we note
     * the task ID and return true.
     */
    public boolean offer(E event, int taskId) throws InterruptedException
    {
        assertNotDone();

        if (predicate.match(event))
        {
            // Log the match for this task.
            this.matched[taskId] = true;

            // If we are not completed we can just return true.
            for (boolean match : matched)
            {
                if (!match)
                    return true;
            }

            // All expected tasks have matched, so we need to report
            // we are done.
            responseQueue.put(new EventHolder(event));
            done = true;

            return true;
        }
        else
            return false;
    }

    private void assertNotDone()
    {
        if (done)
            throw new IllegalStateException(
                    "Operation submitted after watch completion");

    }
}