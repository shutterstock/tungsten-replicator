/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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

import java.util.HashMap;
import java.util.Map;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Tracks the sequence number and time interval between a group of task threads
 * processing transactions to ensure that the first and last threads do not get
 * too far apart in the log. Class methods are fully synchronized, which results
 * in a large number of lock requests. Changes to these classes should be
 * carefully checked for performance via unit tests.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class AtomicIntervalGuard
{
    // Simple class to hold thread information. The thread ID is the key.
    // The before and after fields are used to implement a linked list of
    // threads to show ordering by sequence number.
    private class ThreadPosition
    {
        Thread         thread;
        long           seqno;
        long           time;
        ThreadPosition before;
        ThreadPosition after;

        public String toString()
        {
            return this.getClass().getSimpleName() + " thread=" + thread
                    + " seqno=" + seqno + " time=" + time;
        }
    }

    // Map to hold information on each thread.
    private Map<Thread, ThreadPosition> array = new HashMap<Thread, ThreadPosition>();
    private ThreadPosition              head;
    private ThreadPosition              tail;
    private int                         size;

    /**
     * Allocates a thread interval array.
     * 
     * @param size Expected number of threads for correct operation
     */
    public AtomicIntervalGuard(int size)
    {
        this.size = size;
    }

    /**
     * Report position for an individual thread. This call makes an important
     * assumption that sequence numbers never move backward, which simplifies
     * maintenance of the array.
     * 
     * @throws ReplicatorException Thrown if there is an illegal update.
     */
    public synchronized void report(Thread t, long seqno, long time)
            throws ReplicatorException
    {
        ThreadPosition tp = array.get(t);

        // See if this thread is already known.
        if (tp == null)
        {
            // It is not. Allocate and add to the hash map.
            tp = new ThreadPosition();
            tp.thread = t;
            tp.seqno = seqno;
            tp.time = time;
            array.put(t, tp);

            // Order within the linked list.
            if (head == null)
            {
                // We are starting a new list. This instance is now head and
                // tail.
                head = tp;
                tail = tp;
            }
            else
            {
                // We are inserting in an existing list.
                ThreadPosition nextTp = head;
                while (nextTp != null)
                {
                    // If the next item in the list has a higher sequence
                    // number, we insert before it.
                    if (nextTp.seqno > tp.seqno)
                    {
                        if (nextTp.before != null)
                            nextTp.before.after = tp;
                        tp.before = nextTp.before;
                        tp.after = nextTp;
                        nextTp.before = tp;
                        break;
                    }
                    nextTp = nextTp.after;
                }
                // If we did not find anything, we are at the tail.
                if (nextTp == null)
                {
                    tail.after = tp;
                    tp.before = tail;
                    tail = tp;
                }

                // If we do not have anything before update our position to be
                // head of the list.
                if (tp.before == null)
                    head = tp;
            }
        }
        else
        {
            // The thread is already in the map. Ensure thread seqno does not
            // move backwards and update its information.
            if (tp.seqno > seqno)
                bug("Thread reporting position moved backwards: thread="
                        + t.getName() + " previous seqno=" + tp.seqno
                        + " new seqno=" + seqno);
            tp.seqno = seqno;
            tp.time = time;

            // Since seqno values only increase, we may need to move back in the
            // list. See if we need to move back now.
            ThreadPosition nextTp = tp.after;
            while (nextTp != null && tp.seqno > tp.after.seqno)
            {
                // First fix up nodes before and after this pair so they
                // point to each other.
                if (tp.before != null)
                    tp.before.after = nextTp;
                if (nextTp.after != null)
                    nextTp.after.before = tp;

                // Now switch the pointers on the nodes themselves.
                nextTp.before = tp.before;
                tp.after = nextTp.after;
                nextTp.after = tp;
                tp.before = nextTp;

                // See if we were at the head. If so, move the switched
                // item to the head.
                if (head == tp)
                    head = nextTp;

                // Move to the next item in the linked list.
                nextTp = tp.after;
            }

            // See if we are now at the tail. If so, update the tail.
            if (tp.after == null)
                tail = tp;
        }

        // Notify anyone who is waiting.
        notifyAll();
    }

    /**
     * Get lowest seqno in the array.
     */
    public synchronized long getLowSeqno()
    {
        if (head == null)
            return -1;
        else
            return head.seqno;
    }

    /** Return the lowest time in the array. */
    public synchronized long getLowTime()
    {
        if (head == null)
            return -1;
        else
            return head.time;
    }

    /**
     * Get highest seqno in the array.
     */
    public synchronized long getHiSeqno()
    {
        if (tail == null)
            return -1;
        else
            return tail.seqno;
    }

    /** Return the highest time in the array. */
    public synchronized long getHiTime()
    {
        if (tail == null)
            return -1;
        else
            return tail.time;
    }

    /** Return the interval between highest and lowest values. */
    public synchronized long getInterval()
    {
        return getHiTime() - getLowTime();
    }

    /**
     * Wait until the minimum time in array is greater than or equal to the
     * request time.
     */
    public synchronized long waitMinTime(long time) throws InterruptedException
    {
        assertArrayReady();
        while (time > head.time)
        {
            wait();
        }
        return head.time;
    }

    // Assert that array is full.
    private void assertArrayReady()
    {
        if (size != array.size())
            throw new RuntimeException("Invalid access to array: size=" + size
                    + " actual=" + array.size());
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public synchronized String toString()
    {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getClass().getSimpleName());
        sb.append(" size=").append(size);
        if (array.size() > 0)
        {
            sb.append(" low_seqno=").append(head.seqno);
            sb.append(" hi_seqno=").append(tail.seqno);
            sb.append(" time_interval=").append(tail.time - head.time);
        }
        return sb.toString();
    }

    /**
     * Ensures that the array is consistent by checking various safety
     * conditions. (Where's Eiffel when you need it?)
     */
    public synchronized void validate() throws RuntimeException
    {
        if (head == null)
        {
            if (tail != null)
                bug("Head is null but tail is set");
            else if (array.size() != 0)
                bug("Array is size > 0 when head and tail are empty");
        }
        else
        {
            if (tail == null)
                bug("Head is set but not tail");
            else if (head.before != null)
                bug("Head position points to previous position: " + head.after);
            else if (tail.after != null)
                bug("Tail position points to following position: " + tail.after);
            else
            {
                ThreadPosition tp = head;
                int linkedSize;
                for (linkedSize = 1; linkedSize < array.size(); linkedSize++)
                {
                    if (tp.after == null)
                        break;
                    else
                        tp = tp.after;
                }

                if (linkedSize != array.size())
                    bug("Linked size is different from array size: linked="
                            + linkedSize + " array=" + array.size());

                if (tp != tail)
                    bug("Last item in list is not the tail: last=[" + tp
                            + "] tail=[" + tail + "]");
            }
        }
    }

    // Throw an exception with a bug message.
    private void bug(String message)
    {
        throw new RuntimeException("BUG: " + message);
    }
}