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
