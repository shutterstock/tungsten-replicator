/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * Contact: tungsten@continuent.com
 *
 * This program is property of Continuent.  All rights reserved. 
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.thl.THLEvent;

/**
 * Implements a log reader task that can be used to test concurrent
 * reading and writing. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleLogReader implements Runnable
{
    private static Logger logger = Logger.getLogger(SimpleLogReader.class);
    DiskLog               log;
    long                  startSeqno;
    int              
    howMany;
    int                   eventsRead;
    Throwable             error;

    /** Store file instance. */
    SimpleLogReader(DiskLog log, long startSeqno, int howMany)
    {
        this.log = log;
        this.startSeqno = startSeqno;
        this.howMany = howMany;
    }

    /** Read all records from file. */
    public void run()
    {
        for (long seqno = startSeqno; seqno < startSeqno + howMany; seqno++)
        {
            try
            {
                THLEvent e = log.find(seqno, (short) 0);
                if (e == null)
                    throw new Exception("Event is null: seqno=" + seqno);
                if (seqno != e.getSeqno())
                {
                    throw new Exception(
                            "Sequence numbers do not match: expected=" + seqno
                                    + " actual=" + e.getSeqno());
                }
                eventsRead++;

                if (eventsRead > 0 && eventsRead % 1000 == 0)
                {
                    logger.info("Reading events: threadId="
                            + Thread.currentThread().getId() + " events="
                            + eventsRead);
                }
            }
            catch (Throwable t)
            {
                error = t;
                break;
            }
        }
    }
}