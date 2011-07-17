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

package com.continuent.tungsten.replicator.thl;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

/**
 * Implements a simple unit test EventsCache class to ensure cache behaves and
 * does not block, etc.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class EventsCacheTest extends TestCase
{
    static Logger logger = Logger.getLogger(EventsCacheTest.class);

    /**
     * Verify that a 0-size cache always accepts values and always returns null.
     */
    public void testZeroLengthCache() throws Exception
    {
        EventsCache ec = new EventsCache(0);
        assertTrue("New cache is empty", ec.isEmpty());

        for (int i = 0; i < 100; i++)
        {
            THLEvent ev = makeTHLEvent(i);
            ec.put(ev);
            assertTrue("Zero-length cache is always empty", ec.isEmpty());
            assertNull("Always returns null value", ec.get(i));
        }
    }

    /**
     * Verify that a cache of length N accepts N events and then ages out events
     * to make room for new ones in FIFO order.
     */
    public void testCacheSemantics() throws Exception
    {
        int size = 10;
        EventsCache ec = new EventsCache(size);
        assertTrue("New cache is empty", ec.isEmpty());

        for (int i = 1; i < 100; i++)
        {
            THLEvent ev = makeTHLEvent(i);
            ec.put(ev);

            // Confirm we can get our event back.
            THLEvent ev1 = ec.get(ev.getSeqno());
            assertNotNull("Most recent event always in cache", ev1);
            assertEquals("Found event has correct content",
                    new Long(i).toString(), ev1.getEventId());

            // Confirm FIFO aging using inductive proof.
            if (i <= size)
            {
                THLEvent ec1 = ec.get(1);
                assertNotNull("First event still in", ec1);
            }
            else
            {
                // Last N events are still there...
                int last = i - size + 1;
                THLEvent evSize = ec.get(last);
                assertNotNull("Last N events always in cache", evSize);
                assertEquals("Found event has correct content",
                        new Long(last).toString(), evSize.getEventId());

                // Older events have aged out.
                THLEvent evSizePlus = ec.get(i - size);
                assertNull("Older event is aged out", evSizePlus);
            }
        }
    }

    // Creates a dummy THL event.
    private THLEvent makeTHLEvent(long seqno)
    {
        return new THLEvent(seqno, (short) 0, true, "test", (short) 0, 0, null,
                null, new Long(seqno).toString(), "#UNKNOWN", null);
    }
}