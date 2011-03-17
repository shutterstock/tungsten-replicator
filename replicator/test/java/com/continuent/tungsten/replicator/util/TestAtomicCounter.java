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

import junit.framework.TestCase;

/**
 * Test for AtomicCounter class. Believe it or not, there were two bugs in the
 * original implementation, so this test is very helpful.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestAtomicCounter extends TestCase
{
    /**
     * Show that single-threaded operations work as expected when waiting for
     * great and lesser values.
     */
    public void testSingleThreaded() throws Exception
    {
        // Ensure we can wait for new values.
        AtomicCounter c = new AtomicCounter(0);
        c.waitSeqnoGreaterEqual(0);
        c.waitSeqnoGreaterEqual(-1);
        c.waitSeqnoLessEqual(0);
        c.waitSeqnoLessEqual(1);
        assertEquals("New counter is 0", 0, c.getSeqno());

        // Increment and get value.
        long v1 = c.incrAndGetSeqno();
        assertEquals("Incremented counter is 1", 1, v1);
        assertEquals("Returned value matches increment value", v1, c.getSeqno());

        // Decrement and wait.
        long v2 = c.decrAndGetSeqno();
        assertEquals("Decremented counter is 0", 0, v2);
        assertEquals("Returned value matches increment value", v2, c.getSeqno());

        // Set and wait.
        // Increment and wait.
        c.setSeqno(99);
        assertEquals("Returned value matches set value", 99, c.getSeqno());
    }

    /** Show that multi-threaded increment operations work as expected. */
    public void testMultiThreadIncrement() throws Exception
    {
        // Ensure we can wait for new values.
        final AtomicCounter counter = new AtomicCounter(0);

        for (int i = 0; i < 10; i++)
        {
            // Delete.
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    counter.incrAndGetSeqno();
                }
            };
            new Thread(runnable).start();
        }

        counter.waitSeqnoGreaterEqual(10);
        assertEquals("Thread should be incremented to 10", 10, counter
                .getSeqno());
    }

    /** Show that multi-threaded decrement operations work as expected. */
    public void testMultiThreadDecrement() throws Exception
    {
        // Ensure we can wait for new values.
        final AtomicCounter counter = new AtomicCounter(10);

        for (int i = 0; i < 10; i++)
        {
            // Delete.
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    counter.decrAndGetSeqno();
                }
            };
            new Thread(runnable).start();
        }

        counter.waitSeqnoLessEqual(0);
        assertEquals("Thread should be decremented to 0", 0, counter.getSeqno());
    }

    /** Show that multi-threaded set operations work as expected. */
    public void testMultiThreadSet() throws Exception
    {
        // Ensure we can wait for new values.
        final AtomicCounter counter = new AtomicCounter(0);

        // Delete.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                for (int i = 0; i <= 100000; i++)
                {
                    counter.setSeqno(i);
                }
            }
        };
        new Thread(runnable).start();

        counter.waitSeqnoGreaterEqual(100000);
        assertEquals("Thread should be set to 100000", 100000, counter.getSeqno());
    }
}
