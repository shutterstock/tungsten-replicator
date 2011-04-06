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

import java.io.File;

/**
 * Provides utility methods to open log files quickly.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogHelper
{
    /**
     * Create and return a writable file. Any existing file is deleted.
     */
    static LogFile createLogFile(String name, long seqno) throws Exception
    {
        // Create the file and return it if we want to write.
        File logfile = new File(name);
        logfile.delete();
        LogFile tf = new LogFile(logfile);
        tf.prepareWrite(seqno);
        return tf;
    }

    /**
     * Open an existing file for reading.
     */
    static LogFile openExistingFileForRead(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        tf.prepareRead();
        return tf;
    }

    /**
     * Open an existing file for writing.
     */
    static LogFile openExistingFileForWrite(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        if (tf.prepareWrite(-1))
            throw new Exception(
                    "Unexpectedly created a supposedly existin file: "
                            + tf.getFile().getName());
        return tf;
    }
}
