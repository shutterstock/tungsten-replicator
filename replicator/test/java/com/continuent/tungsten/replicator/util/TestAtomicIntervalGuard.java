/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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

import org.apache.log4j.Logger;

import junit.framework.TestCase;

/**
 * Test for AtomicIntervalGuard class. In additional to catching bugs in the
 * queue structure (this test found several) the last case gives an indication
 * of performance by simulating the usage of the AtomicIntervalGuard in the
 * replicator. We should be able to handle 30 threads simultaneously reporting
 * on 1M independent transactions within 10 seconds. Recent runs on Mac OS X
 * laptop with Intel Core 2 processor complete in about 9.7s. Any result over
 * 10s should be investigated.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestAtomicIntervalGuard extends TestCase
{
    private static Logger logger = Logger.getLogger(TestAtomicIntervalGuard.class);

    /**
     * Show that single threaded operation correctly inserts values.
     */
    public void testInitialInsert() throws Exception
    {
        // Allocate interval array and threads to live in it.
        AtomicIntervalGuard ati = new AtomicIntervalGuard(3);
        Thread[] t = {new Thread("t0"), new Thread("t1"), new Thread("t2")};

        // Ensure that new threads insert correctly.
        ati.report(t[0], 2, 20);
        assertEquals("Head #1", 2, ati.getLowSeqno());
        assertEquals("Tail #1", 2, ati.getHiSeqno());
        assertEquals("Head #1 -time", 20, ati.getLowTime());
        assertEquals("Tail #1 -time", 20, ati.getHiTime());
        ati.validate();
        assertEquals("Interval #1", 0, ati.getInterval());

        ati.report(t[1], 1, 10);
        assertEquals("Head #2", 1, ati.getLowSeqno());
        assertEquals("Tail #2", 2, ati.getHiSeqno());
        assertEquals("Head #2 -time", 10, ati.getLowTime());
        assertEquals("Tail #2 -time", 20, ati.getHiTime());
        assertEquals("Interval #2", 10, ati.getInterval());
        ati.validate();

        ati.report(t[2], 3, 30);
        assertEquals("Head #3", 1, ati.getLowSeqno());
        assertEquals("Tail #3", 3, ati.getHiSeqno());
        assertEquals("Head #3 -time", 10, ati.getLowTime());
        assertEquals("Tail #3 -time", 30, ati.getHiTime());
        assertEquals("Interval #3", 20, ati.getInterval());
        ati.validate();
    }

    /**
     * Show that single threaded operation correctly reorders values.
     */
    public void testReordering() throws Exception
    {
        // Allocate interval array and threads to live in it.
        AtomicIntervalGuard ati = new AtomicIntervalGuard(3);
        Thread[] t = {new Thread("t0"), new Thread("t1"), new Thread("t2")};

        // Add initial data. (Thread order = 0,1,2)
        ati.report(t[0], 1, 10);
        ati.report(t[1], 2, 20);
        ati.report(t[2], 3, 30);
        ati.validate();

        // Test moving from head to tail. (Final thread order = 1,2,0)
        ati.report(t[0], 8, 80);
        assertEquals("Head #1", 2, ati.getLowSeqno());
        assertEquals("Head #1 -time", 20, ati.getLowTime());
        assertEquals("Tail #1", 8, ati.getHiSeqno());
        assertEquals("Tail #1 -time", 80, ati.getHiTime());
        assertEquals("Interval #1", 60, ati.getInterval());
        ati.validate();

        // Test not moving from middle. (Final thread order 1,2,0)
        ati.report(t[2], 7, 70);
        assertEquals("Head #2", 2, ati.getLowSeqno());
        assertEquals("Head #2 -time", 20, ati.getLowTime());
        assertEquals("Tail #2", 8, ati.getHiSeqno());
        assertEquals("Tail #2 -time", 80, ati.getHiTime());
        assertEquals("Interval #2", 60, ati.getInterval());
        ati.validate();

        // Test not moving from head. (Final thread order 1,2,0)
        ati.report(t[1], 6, 60);
        assertEquals("Head #3", 6, ati.getLowSeqno());
        assertEquals("Head #3 -time", 60, ati.getLowTime());
        assertEquals("Tail #3", 8, ati.getHiSeqno());
        assertEquals("Tail #3 -time", 80, ati.getHiTime());
        assertEquals("Interval #3", 20, ati.getInterval());

        // Test not moving from tail. (Final thread order 1,2,0)
        ati.report(t[0], 9, 90);
        assertEquals("Head #4", 6, ati.getLowSeqno());
        assertEquals("Head #4 -time", 60, ati.getLowTime());
        assertEquals("Tail #4", 9, ati.getHiSeqno());
        assertEquals("Tail #4 -time", 90, ati.getHiTime());
        assertEquals("Interval #4", 30, ati.getInterval());
        ati.validate();

        // Test moving from middle. (Final thread order 1,0,2)
        ati.report(t[2], 10, 100);
        assertEquals("Head #5", 6, ati.getLowSeqno());
        assertEquals("Head #5 -time", 60, ati.getLowTime());
        assertEquals("Tail #5", 10, ati.getHiSeqno());
        assertEquals("Tail #5 -time", 100, ati.getHiTime());
        assertEquals("Interval #4", 40, ati.getInterval());
        ati.validate();
    }

    /**
     * Show that time intervals are correctly calculated and that we can wait
     * for them.
     */
    public void testSingleThreadedInterval() throws Exception
    {
        // Allocate interval array and threads to live in it.
        AtomicIntervalGuard ati = new AtomicIntervalGuard(3);
        Thread[] t = {new Thread("t0"), new Thread("t1"), new Thread("t2")};

        // Perform 100K iterations of updating all threads and waiting for
        // spread. At the end of each iteration the threads should be within
        // 20ms of each other.
        long seqno = 0;
        long time = 0;
        for (int i = 0; i < 100000; i++)
        {
            // Increment each thread.
            for (int j = 0; j < 3; j++)
            {
                seqno++;
                time += 10;
                ati.report(t[j], seqno, time);
            }
            // Verify time interval.
            long minimumTime = Math.max(time - 20, 0);
            long tailTime = ati.waitMinTime(minimumTime);
            assertEquals("Tail time at iteration: " + i, minimumTime, tailTime);
        }
    }

    /**
     * Show that time intervals work correctly when a large number of threads
     * whose progress is gated by an atomic counter are simultaneously updating
     * the thread interval array.
     */
    public void testMultiThreadedInterval() throws Exception
    {
        // Allocate interval array and threads to live in it.
        AtomicIntervalGuard ati = new AtomicIntervalGuard(30);
        AtomicCounter counter = new AtomicCounter(0);
        SampleThreadIntervalWriter[] writer = new SampleThreadIntervalWriter[30];
        for (int i = 0; i < writer.length; i++)
        {
            writer[i] = new SampleThreadIntervalWriter(counter, ati, 1000000);
            ati.report(writer[i], 0, 0);
            writer[i].start();
        }

        // Advance the counter only if the thread interval remains within
        // 5000 of sequence number. This simulates a spread of 5000 ms.
        long startMillis = System.currentTimeMillis();
        for (;;)
        {
            long seqno = counter.incrAndGetSeqno();
            if (seqno >= 1000000)
                break;
            ati.waitMinTime(Math.max(seqno - 5000, 0));
            if (seqno % 50000 == 0)
            {
                double elapsed = (System.currentTimeMillis() - startMillis) / 1000.0;
                logger.info("Processed seqno=" + seqno + " elapsed=" + elapsed);
            }
        }
        double elapsed = (System.currentTimeMillis() - startMillis) / 1000.0;
        logger.info("Processed seqno=" + counter.getSeqno() + " elapsed="
                + elapsed);

        // Ensure all threads to complete. This should happen within 60
        // seconds.
        for (int i = 0; i < writer.length; i++)
        {
            writer[i].join(60000);
            if (writer[i].throwable != null)
            {
                // Writer hit an error.
                throw new Exception("Writer terminated abnormally: writer=" + i
                        + " seqno=" + writer[i].seqno, writer[i].throwable);
            }
            if (!writer[i].done)
            {
                // Writer did not finish--could be hung!
                throw new Exception("Writer did not terminate: writer=" + i
                        + " seqno=" + writer[i].seqno);
            }

            // Make sure we finished expected # of transactions.
            assertEquals("Checking writer[" + i + "] seqno", 1000000,
                    writer[i].seqno);
        }
    }
}

// Sample class to post thread position.
class SampleThreadIntervalWriter extends Thread
{
    private AtomicCounter       counter;
    private AtomicIntervalGuard threadInterval;
    private long                maxSeqno;

    volatile Throwable          throwable;
    volatile long               seqno;
    volatile boolean            done;

    /**
     * Create new write with counter and maximum sequence number to execute to.
     * (Start at 0.)
     */
    SampleThreadIntervalWriter(AtomicCounter counter,
            AtomicIntervalGuard threadInterval, long maxSeqno)
    {
        this.counter = counter;
        this.maxSeqno = maxSeqno;
        this.threadInterval = threadInterval;
    }

    /**
     * Execute a loop to wait and report sequence numbers.
     */
    public void run()
    {
        seqno = 0;
        Thread currentThread = Thread.currentThread();

        try
        {
            for (;;)
            {
                // Increment seqno, breaking out of loop if we already are at
                // the maximum.
                if (seqno >= maxSeqno)
                    break;
                seqno++;

                // Wait for green light to report sequence number.
                counter.waitSeqnoGreaterEqual(seqno);

                // Report the sequence number.
                threadInterval.report(currentThread, seqno, seqno);
            }
        }
        catch (InterruptedException e)
        {
        }
        catch (Throwable t)
        {
            throwable = t;
        }
        finally
        {
            done = true;
        }
    }
}