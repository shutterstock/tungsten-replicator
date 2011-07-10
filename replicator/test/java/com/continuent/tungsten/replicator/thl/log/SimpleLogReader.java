/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
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

package com.continuent.tungsten.replicator.thl.log;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.util.AtomicCounter;

/**
 * Implements a log reader task that can be used to test concurrent reading and
 * writing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleLogReader implements Runnable
{
    private static Logger logger = Logger.getLogger(SimpleLogReader.class);
    DiskLog               log;
    final long            startSeqno;
    final int             howMany;
    volatile int          eventsRead;
    final AtomicCounter   lastSeqno;
    Throwable             error;
    Thread                myThread;

    /** Store file instance. */
    SimpleLogReader(DiskLog log, long startSeqno, int howMany)
    {
        this.log = log;
        this.startSeqno = startSeqno;
        this.howMany = howMany;
        this.lastSeqno = new AtomicCounter(-1);
    }

    /** Read all records from file. */
    public void run()
    {
        myThread = Thread.currentThread();
        try
        {
            LogConnection conn = log.connect(true);
            conn.seek(startSeqno);
            for (long seqno = startSeqno; seqno < startSeqno + howMany; seqno++)
            {
                THLEvent e = conn.next();
                if (e == null)
                    throw new Exception("Event is null: seqno=" + seqno);
                if (seqno != e.getSeqno())
                {
                    throw new Exception(
                            "Sequence numbers do not match: expected=" + seqno
                                    + " actual=" + e.getSeqno());
                }
                eventsRead++;
                lastSeqno.setSeqno(e.getSeqno());

                if (eventsRead > 0 && eventsRead % 1000 == 0)
                {
                    logger.info("Reading events: threadId="
                            + Thread.currentThread().getId() + " events="
                            + eventsRead);
                }
            }
        }
        catch (InterruptedException e)
        {
        }
        catch (Throwable t)
        {
            error = t;
        }
    }

    /** Wait until the thread is done or at least try to. */
    public boolean waitFinish(long timeout)
    {
        if (myThread == null)
            return false;
        else
        {
            try
            {
                myThread.join(timeout);
            }
            catch (InterruptedException e)
            {
            }
            return !myThread.isAlive();
        }
    }
}