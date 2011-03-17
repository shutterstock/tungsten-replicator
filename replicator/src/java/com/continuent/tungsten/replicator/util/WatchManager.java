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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.util;

import java.util.List;
import java.util.Vector;

/**
 * Manages a list of event watches and allows clients to submit events to the
 * list for processing to see if there is a predicate match. Methods are
 * synchronized to ensure the object is updated transactionally and to ensure
 * proper visibility across threads.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class WatchManager<E>
{
    private List<Watch<E>> watchList = new Vector<Watch<E>>();
    boolean                cancelled = false;

    public WatchManager()
    {
    }

    /**
     * Adds a new watch predicate to the queue including an accompanying action.
     */
    public synchronized Watch<E> watch(WatchPredicate<E> predicate,
            int taskCount, WatchAction<E> action)
    {
        assertNotCancelled();
        Watch<E> watch = new Watch<E>(predicate, taskCount, action);
        watchList.add(watch);
        return watch;
    }

    /**
     * Adds a new watch predicate to the queue.
     */
    public synchronized Watch<E> watch(WatchPredicate<E> predicate,
            int taskCount)
    {
        return watch(predicate, taskCount, null);
    }

    /**
     * Submits an event for watch processing. This automatically dequeues any
     * matching watch instances and informs the watchers.
     * 
     * @param event An event for processing.
     * @param taskId Id of task for which we are checking the predicate
     * @throws InterruptedException
     */
    public synchronized void process(E event, int taskId)
            throws InterruptedException
    {
        assertNotCancelled();
        // Walk backwards down list to avoid ConcurrentModificationException
        // from using an Iterator. Note we also clean out anything that is
        // done; this is how cancelled watches are removed.
        for (int i = watchList.size() - 1; i >= 0; i--)
        {
            Watch<E> watch = watchList.get(i);
            if (watch.isDone())
                watchList.remove(watch);
            else if (watch.offer(event, taskId))
            {
                // Execute the watch action.
                WatchAction<E> action = watch.getAction();
                if (action != null)
                {
                    action.matched(event, taskId);
                }

                // Dequeue if watch is fulfilled.
                if (watch.isDone())
                    watchList.remove(watch);
            }
        }
    }

    /**
     * Cancel all pending watches.
     */
    public synchronized void cancelAll()
    {
        assertNotCancelled();
        for (Watch<E> w : watchList)
        {
            w.cancel(true);
        }
        cancelled = true;
    }

    private void assertNotCancelled()
    {
        if (cancelled)
            throw new IllegalStateException(
                    "Operation submitted after cancellation");
    }
}