/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
 * Contact: tungsten@continuent.com
 *
 * This program is property of Continuent.  All rights reserved. 
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

/**
 * Log reader task used for log file reading tests.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class SimpleLogFileReader implements Runnable
{
    LogFile   tf;
    long      howMany;
    long      recordsRead;
    long      bytesRead;
    long      crcFailures;
    Exception error;

    /** Store file instance. */
    public SimpleLogFileReader(LogFile tf, long maxRecords)
    {
        this.tf = tf;
        this.howMany = maxRecords;
    }

    /** Read all records from file. */
    public void run()
    {
        while (recordsRead < howMany)
        {
            try
            {
                // Read until we run out of records or hit exception.
                LogRecord rec;
                rec = tf.readRecord(2000);
                if (rec.isEmpty())
                    break;

                // Update counters.
                recordsRead++;
                bytesRead += rec.getRecordLength();
                if (!rec.checkCrc())
                    crcFailures++;
            }
            catch (Exception e)
            {
                error = e;
                break;
            }
        }
    }
}
