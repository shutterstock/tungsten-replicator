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
 * Initial developer(s): Teemu Ollakka, Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.util;

import org.apache.log4j.Logger;

/**
 * Defines a simple "atomic counter" that allows clients to increment the
 * encapsulated sequence number and wait synchronously until particular values
 * are reached. This class is thread-safe.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class AtomicCounter
{
    private static Logger logger = Logger.getLogger(AtomicCounter.class);
    private long          seqno;

    /**
     * Creates a new <code>Sequencer</code> object with a starting value.
     * 
     * @param seqno Initial sequence number
     */
    public AtomicCounter(long seqno)
    {
        this.seqno = seqno;
    }

    /**
     * Get value of current seqno.
     * 
     * @return Seqno
     */
    public synchronized long getSeqno()
    {
        return seqno;
    }

    /**
     * Sets values of current seqno. Value can only be set upward.
     */
    public synchronized void setSeqno(long seqno)
    {
        if (this.seqno < seqno)
        {
            this.seqno = seqno;
            notifyAll();
        }
    }

    /**
     * Increment seqno and notify waiters, then return value.
     */
    public synchronized long incrAndGetSeqno()
    {
        seqno++;
        notifyAll();
        return seqno;
    }

    /**
     * Decrement seqno and notify waiters, then return value.
     */
    public synchronized long decrAndGetSeqno()
    {
        seqno--;
        notifyAll();
        return seqno;
    }

    /**
     * Wait until seqno is greater than or equal to the desired value.
     * 
     * @param waitSeqno Sequence number to wait for
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized void waitSeqnoGreaterEqual(long waitSeqno)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);
        while (waitSeqno > seqno)
            this.wait();
    }

    /**
     * Wait until seqno is greater than or equal to the desired value *or* we
     * exceed the timeout.
     * 
     * @param waitSeqno Sequence number to wait for
     * @param millis Number of milliseconds to wait
     * @returns True if wait was successful, otherwise false
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized boolean waitSeqnoGreaterEqual(long waitSeqno,
            long millis) throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);

        // Compute end time.
        long startMillis = System.currentTimeMillis();
        long endMillis = startMillis + millis;

        // Loop until the end time is met or exceeded.
        while (waitSeqno > seqno)
        {
            this.wait(millis);
            long currentMillis = System.currentTimeMillis();
            millis = endMillis - currentMillis;
            if (millis <= 0)
                break;
        }

        // Return true if we achieved the desired sequence number.
        return (waitSeqno <= seqno);
    }

    /**
     * Wait until seqno is less than or equal to the desired value.
     * 
     * @param waitSeqno Sequence number to wait for
     * @throws InterruptedException if somebody cancels the wait
     */
    public synchronized void waitSeqnoLessEqual(long waitSeqno)
            throws InterruptedException
    {
        if (logger.isDebugEnabled())
            logger.debug("Waiting for sequence number: " + waitSeqno);
        while (waitSeqno < seqno)
            this.wait();
    }

    /**
     * Print a string representation of the value.
     */
    public synchronized String toString()
    {
        return this.getClass().toString() + " [" + seqno + "]";
    }
}
